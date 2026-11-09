//! An update of the session daemon from build N to build N+1, where N+1
//! bumps the agent roster reducer version, on a registry of millions of
//! journal records. Leo, 2026-11-09: "for these kinds of things, updates
//! need to be able to autorepair instead of getting the user to do this or
//! that to fix or troubleshoot".
//!
//! The registry is the synthetic generator's (`cmux_tui_core::synth_registry`,
//! built from this tree, so its roster snapshot is build N's): mostly
//! `personal.session.put`, some legacy `terminal.output`, about 1% `agent.*`
//! hook events, the first half sealed into segments, and the roster snapshot
//! at the head at build N's reducer version, as build N leaves it.
//!
//! - Baseline: build N serves the registry; it stops as the app's update
//!   stops it; build N+1 starts and re-folds the roster.
//! - Premigrate: build N serves while a client keeps writing to it, and
//!   build N+1's `registry premigrate` folds the roster beside it; build N
//!   answers every probe (about 1 s apart) and keeps its pid. Then build N
//!   stops and build N+1 starts on the premigrated registry.
//!
//! In both, while build N+1 starts, `server ensure` runs every few seconds as
//! the app runs it and never replaces the owner, and an action the person
//! made before the owner answered is held and replayed exactly once after it
//! answers (`cmux_daemon_conn_core::offline`, the app's rule). The restore
//! finishes on its own: the roster snapshot reaches N+1's version at the
//! journal head, and its entries are the generator's fold of the same
//! records.
//!
//! Not a regular test: run by `scripts/e2e/update-premigrate.sh`, which
//! builds both binaries. By hand:
//!
//! ```sh
//! CMUX_E2E_OLD_BIN=/w/cmux-tui-n CMUX_E2E_NEW_BIN=/w/cmux-tui-n1 \
//!   CMUX_E2E_RECORDS=300000 CMUX_E2E_DIR=/w/e2e \
//!   cargo test --release -p cmux-tui --test update_premigrate_e2e -- --ignored --nocapture
//! ```
//!
//! Environment: `CMUX_E2E_OLD_BIN` (build N, default this tree's binary),
//! `CMUX_E2E_NEW_BIN` (build N+1, required), `CMUX_E2E_RECORDS` (default
//! 3000000), `CMUX_E2E_DIR` (default the temp dir), `CMUX_E2E_NEW_VERSION`
//! (N+1's reducer version, default N's plus one), `CMUX_E2E_PAYLOAD_BYTES`
//! (one session put, default 1024), `CMUX_E2E_DEADLINE_S` (each wait,
//! default 3600), `CMUX_E2E_RESULT_FILE` (the summary line is written there
//! too).

#![cfg(target_os = "linux")]

use std::fs;
use std::io::{BufRead as _, BufReader, Write as _};
use std::os::unix::fs::PermissionsExt as _;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use cmux_daemon_conn_core::offline;
use cmux_tui_core::synth_registry::{SynthOptions, generate, registry_path};
use rusqlite::{Connection, OpenFlags};
use serde_json::{Value, json};

const SESSION: &str = "update-e2e";
/// The app's interval between `server ensure` runs is minutes; a few
/// seconds here meets every phase of the start.
const ENSURE_EVERY: Duration = Duration::from_secs(3);
/// How often the old owner is probed while the premigrate runs.
const PROBE_EVERY: Duration = Duration::from_secs(1);
/// One probe of the old owner fails past this.
const PROBE_BOUND: Duration = Duration::from_secs(10);
/// Output cleanup is its own change; it never starts during this test.
const NEVER_MS: &str = "86400000";

struct Settings {
    old_bin: PathBuf,
    new_bin: PathBuf,
    records: u64,
    root: PathBuf,
    new_version: Option<u64>,
    payload_bytes: usize,
    deadline: Duration,
    result_file: Option<PathBuf>,
    /// Debugging only: the premigrate scenario is skipped.
    baseline_only: bool,
}

impl Settings {
    fn from_env() -> Self {
        let var = |name: &str| std::env::var(name).ok().filter(|value| !value.is_empty());
        let number = |name: &str| {
            var(name).map(|value| {
                value
                    .replace('_', "")
                    .parse::<u64>()
                    .unwrap_or_else(|_| panic!("{name}={value} is not a number"))
            })
        };
        Self {
            old_bin: var("CMUX_E2E_OLD_BIN")
                .map_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_cmux-tui")), PathBuf::from),
            new_bin: PathBuf::from(
                var("CMUX_E2E_NEW_BIN").expect("CMUX_E2E_NEW_BIN names build N+1 (see the doc)"),
            ),
            records: number("CMUX_E2E_RECORDS").unwrap_or(3_000_000),
            root: var("CMUX_E2E_DIR").map_or_else(std::env::temp_dir, PathBuf::from),
            new_version: number("CMUX_E2E_NEW_VERSION"),
            payload_bytes: number("CMUX_E2E_PAYLOAD_BYTES").unwrap_or(1024) as usize,
            deadline: Duration::from_secs(number("CMUX_E2E_DEADLINE_S").unwrap_or(3600)),
            result_file: var("CMUX_E2E_RESULT_FILE").map(PathBuf::from),
            baseline_only: var("CMUX_E2E_BASELINE_ONLY").is_some_and(|value| value == "1"),
        }
    }
}

/// One owner's files: its state below `dir`, its socket in a short folder
/// of its own below /tmp (a socket path has at most 107 bytes).
struct Site {
    dir: PathBuf,
    socket_dir: PathBuf,
}

impl Site {
    fn new(root: &Path, label: &str) -> Self {
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let dir = root.join(format!("update-e2e-{}-{label}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("home")).unwrap();
        fs::create_dir_all(dir.join("state")).unwrap();
        fs::set_permissions(&dir, fs::Permissions::from_mode(0o700)).unwrap();
        let socket_dir = PathBuf::from("/tmp").join(format!(
            "cmux-ue-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&socket_dir);
        fs::create_dir_all(&socket_dir).unwrap();
        fs::set_permissions(&socket_dir, fs::Permissions::from_mode(0o700)).unwrap();
        Self { dir, socket_dir }
    }

    /// A copy of `from`'s state (the generated registry) in a site of its own.
    fn copy_of(from: &Site, root: &Path, label: &str) -> Self {
        let site = Self::new(root, label);
        let copied = Instant::now();
        let status = Command::new("cp")
            .args(["-a", "--reflink=auto"])
            .arg(from.state_root())
            .arg(&site.dir)
            .status()
            .unwrap();
        assert!(status.success(), "copying the registry failed");
        eprintln!("e2e: copied the registry for {label} in {:.1} s", copied.elapsed().as_secs_f64());
        site
    }

    fn state_root(&self) -> PathBuf {
        self.dir.join("state")
    }

    fn socket(&self) -> PathBuf {
        self.socket_dir.join("mux.sock")
    }

    fn registry(&self) -> PathBuf {
        registry_path(&self.state_root(), SESSION)
    }

    /// `bin` with this site's state, config, log and home.
    fn command(&self, bin: &Path) -> Command {
        let mut command = Command::new(bin);
        command
            .env("HOME", self.dir.join("home"))
            .env("CMUX_TUI_STATE_DIR", self.state_root())
            .env("CMUX_TUI_CONFIG", self.dir.join("config.json"))
            .env("CMUX_TUI_LOG_FILE", self.dir.join("client.log"))
            .env("CMUX_TUI_TEST_OUTPUT_CLEANUP_DELAY_MS", NEVER_MS)
            .env("CMUX_TUI_TEST_JOURNAL_MAINTENANCE_INTERVAL_MS", NEVER_MS)
            .env_remove("CMUX_TUI_SOCKET")
            .env_remove("CMUX_TAG")
            .stdin(Stdio::null());
        command
    }

    /// `bin server ACTION --json` on this site's socket.
    fn server(&self, bin: &Path, action: &str) -> (bool, Value, String) {
        let output = self
            .command(bin)
            .args(["server", action, "--json", "--session", SESSION, "--socket"])
            .arg(self.socket())
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        let value = last_json(&stdout).or_else(|| last_json(&stderr)).unwrap_or(Value::Null);
        (output.status.success(), value, format!("{stdout}{stderr}"))
    }
}

impl Drop for Site {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
        let _ = fs::remove_dir_all(&self.socket_dir);
    }
}

/// The last line of `text` that is a JSON object.
fn last_json(text: &str) -> Option<Value> {
    text.lines().rev().find_map(|line| {
        serde_json::from_str::<Value>(line.trim()).ok().filter(|value| value.is_object())
    })
}

/// A headless owner, a child of this test.
struct Owner<'a> {
    site: &'a Site,
    child: Child,
    started: Instant,
    log: PathBuf,
    label: String,
}

impl<'a> Owner<'a> {
    fn spawn(site: &'a Site, bin: &Path, label: &str) -> Self {
        let _ = fs::remove_file(site.socket());
        let log = site.dir.join(format!("owner-{label}.log"));
        let child = site
            .command(bin)
            .args(["--headless", "--session", SESSION, "--socket"])
            .arg(site.socket())
            .stdout(Stdio::null())
            .stderr(fs::File::create(&log).unwrap())
            .spawn()
            .unwrap();
        Self { site, child, started: Instant::now(), log, label: label.into() }
    }

    fn pid(&self) -> u64 {
        u64::from(self.child.id())
    }

    fn log_tail(&self) -> String {
        let text = fs::read_to_string(&self.log).unwrap_or_default();
        let lines: Vec<&str> = text.lines().collect();
        lines[lines.len().saturating_sub(40)..].join("\n")
    }

    fn assert_alive(&mut self, doing: &str) {
        if let Some(status) = self.child.try_wait().unwrap() {
            panic!("owner {} exited ({status}) while {doing}:\n{}", self.label, self.log_tail());
        }
    }

    /// `identify`'s data when the owner answers within `bound`.
    fn identify(&self, bound: Duration) -> Option<Value> {
        request(&self.site.socket(), json!({"cmd": "identify"}), bound)
            .filter(|reply| reply["ok"] == true)
            .map(|reply| reply["data"].clone())
    }

    /// Waits until the owner says `lifecycle_ready`; returns the time since spawn.
    fn wait_ready(&mut self, deadline: Duration) -> Duration {
        let until = self.started + deadline;
        loop {
            self.assert_alive("starting");
            if let Some(identity) = self.identify(Duration::from_secs(2))
                && identity["lifecycle_ready"] == true
            {
                assert_eq!(identity["pid"].as_u64(), Some(self.pid()), "another owner answered");
                return self.started.elapsed();
            }
            assert!(Instant::now() < until, "owner {} not ready:\n{}", self.label, self.log_tail());
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn workspace_names(&self) -> Vec<String> {
        let reply = request(
            &self.site.socket(),
            json!({"cmd": "list-workspaces"}),
            Duration::from_secs(30),
        )
        .unwrap_or_else(|| panic!("no answer to list-workspaces:\n{}", self.log_tail()));
        assert_eq!(reply["ok"], true, "list-workspaces: {reply}");
        reply["data"]["workspaces"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|workspace| workspace["name"].as_str().map(str::to_string))
            .collect()
    }

    /// Stops the owner as the app's update does (`server stop`, terminals
    /// kept for the next owner), from build `bin`, and waits for its exit.
    fn hand_off(mut self, bin: &Path) {
        let (ok, value, text) = self.site.server(bin, "stop");
        assert!(ok, "server stop failed: {value} {text}");
        let until = Instant::now() + Duration::from_secs(120);
        while self.child.try_wait().unwrap().is_none() {
            assert!(Instant::now() < until, "owner {} did not exit after server stop", self.label);
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for Owner<'_> {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn next_id() -> u64 {
    static NEXT: AtomicU32 = AtomicU32::new(1);
    u64::from(NEXT.fetch_add(1, Ordering::Relaxed))
}

/// One request on a fresh connection: the first reply with its id, or
/// `None` when the socket refuses or no reply came within `bound`.
fn request(socket: &Path, value: Value, bound: Duration) -> Option<Value> {
    exchange(socket, value, bound).ok()
}

/// Why a request got no reply, as the app's `DaemonError` names it.
fn exchange(socket: &Path, mut value: Value, bound: Duration) -> Result<Value, &'static str> {
    let id = value.get("id").cloned().unwrap_or_else(|| json!(next_id()));
    value["id"] = id.clone();
    let stream = UnixStream::connect(socket).map_err(|_| "connectFailed")?;
    stream.set_read_timeout(Some(bound)).map_err(|_| "connectFailed")?;
    stream.set_write_timeout(Some(bound)).map_err(|_| "connectFailed")?;
    let mut reader = BufReader::new(stream);
    writeln!(reader.get_mut(), "{value}").map_err(|_| "connectionClosed")?;
    let until = Instant::now() + bound;
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => return Err("connectionClosed"),
            Ok(_) => {}
            Err(_) => return Err("timedOut"),
        }
        if Instant::now() > until {
            return Err("timedOut");
        }
        let Ok(reply) = serde_json::from_str::<Value>(&line) else { continue };
        if reply["id"] == id {
            return Ok(reply);
        }
    }
}

/// A `cmux.protocol/2` `workspace.create` of an empty workspace.
fn create_workspace(name: &str, key: &str) -> Value {
    json!({
        "protocol": "cmux.protocol/2", "type": "request", "id": key,
        "operation": "workspace.create",
        "params": {"machine": "current", "session": "current", "name": name,
                   "initial_content": "empty"},
        "idempotency_key": key,
    })
}

fn read_only(registry: &Path) -> Connection {
    Connection::open_with_flags(registry, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap()
}

/// The highest journal sequence, live or sealed (`query_journal_head`).
fn journal_head(registry: &Path) -> u64 {
    read_only(registry)
        .query_row(
            "SELECT MAX(COALESCE((SELECT MAX(sequence) FROM session_journal), 0),
                        COALESCE((SELECT MAX(end_sequence) FROM journal_segments), 0))",
            [],
            |row| row.get::<_, i64>(0),
        )
        .map(|head| head as u64)
        .unwrap()
}

/// The roster snapshot row: version, cursor and its entries.
fn roster(registry: &Path) -> Option<(u64, u64, Value)> {
    let raw: String = read_only(registry)
        .query_row("SELECT value FROM meta WHERE key = 'journal_reducer.agent_roster'", [], |row| {
            row.get(0)
        })
        .ok()?;
    let value: Value = serde_json::from_str(&raw).ok()?;
    let snapshot: Value = serde_json::from_str(value["snapshot"].as_str()?).ok()?;
    Some((value["version"].as_u64()?, value["cursor"].as_str()?.parse().ok()?, snapshot["entries"].clone()))
}

/// `server ensure` as the app runs it, every [`ENSURE_EVERY`], from the
/// moment the new owner listens until it is stopped.
struct EnsureLoop {
    stop: Arc<AtomicBool>,
    runs: Arc<Mutex<Vec<(bool, Value, String)>>>,
    thread: Option<JoinHandle<()>>,
}

impl EnsureLoop {
    fn start(site: &Site, bin: &Path) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let runs = Arc::new(Mutex::new(Vec::new()));
        let mut command = site.command(bin);
        command
            .args(["server", "ensure", "--json", "--session", SESSION, "--socket"])
            .arg(site.socket());
        let thread = {
            let (stop, runs) = (stop.clone(), runs.clone());
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let output = command.output().unwrap();
                    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
                    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
                    let value =
                        last_json(&stdout).or_else(|| last_json(&stderr)).unwrap_or(Value::Null);
                    runs.lock().unwrap().push((output.status.success(), value, stderr));
                    let pause = Instant::now() + ENSURE_EVERY;
                    while Instant::now() < pause && !stop.load(Ordering::Relaxed) {
                        std::thread::sleep(Duration::from_millis(50));
                    }
                }
            })
        };
        Self { stop, runs, thread: Some(thread) }
    }

    /// Stops the loop and asserts that every run found `pid` and replaced
    /// nothing. Returns how many ran.
    fn finish(mut self, pid: u64) -> usize {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
        let runs = self.runs.lock().unwrap();
        for (ok, value, stderr) in runs.iter() {
            assert!(ok, "server ensure failed while the new owner started: {value} {stderr}");
            assert_eq!(value["status"], "running", "server ensure did not find the owner: {value}");
            assert_eq!(value["pid"].as_u64(), Some(pid), "server ensure answered another owner: {value}");
            assert!(value.get("replaced_pid").is_none(), "server ensure replaced the owner: {value}");
        }
        runs.len()
    }
}

/// An action the person made before the new owner answered: held by the
/// app's rule while the owner does not answer, replayed once it does.
struct HeldAction {
    name: String,
    key: String,
    held: Vec<offline::Held>,
    replays: u32,
    landed: bool,
}

impl HeldAction {
    fn new(label: &str) -> Self {
        Self {
            name: format!("held-during-start-{label}"),
            key: format!("e2e-held-{label}"),
            held: Vec::new(),
            replays: 0,
            landed: false,
        }
    }

    /// The person's first try: before the owner is ready it cannot land.
    fn try_first(&mut self, owner: &Owner<'_>) {
        let now = owner.started.elapsed().as_millis() as u64;
        match exchange(
            &owner.site.socket(),
            create_workspace(&self.name, &self.key),
            Duration::from_millis(500),
        ) {
            Ok(reply) if reply["ok"] == true => self.landed = true,
            Ok(reply) => panic!("the owner refused the action: {reply}"),
            Err(kind) => {
                assert_eq!(
                    offline::disposition(kind, false),
                    offline::Disposition::Hold,
                    "the app would drop an action that met {kind}"
                );
                self.held.push(offline::Held { id: self.key.clone(), held_at_ms: now, replaying: false });
            }
        }
    }

    /// The app's sweep at `now_ms` with `reachable`: replays the action once
    /// when the sweep says so; never expires it.
    fn sweep(&mut self, owner: &Owner<'_>, reachable: bool) {
        let now = owner.started.elapsed().as_millis() as u64;
        let sweep = offline::sweep(&self.held, now, reachable, offline::HOLD_BOUND_MS);
        assert!(sweep.expire.is_empty(), "the held action expired before the owner answered");
        for id in sweep.replay {
            assert_eq!(id, self.key);
            self.held.iter_mut().for_each(|held| held.replaying = true);
            self.replays += 1;
            let reply = exchange(
                &owner.site.socket(),
                create_workspace(&self.name, &self.key),
                Duration::from_secs(30),
            )
            .unwrap_or_else(|kind| panic!("the replay failed ({kind}):\n{}", owner.log_tail()));
            assert_eq!(reply["ok"], true, "the replay was refused: {reply}");
            self.held.clear();
            self.landed = true;
        }
    }

    fn assert_once(&self, owner: &Owner<'_>) {
        assert!(self.landed, "the held action never landed");
        assert!(self.replays <= 1, "the held action replayed {} times", self.replays);
        let count = owner.workspace_names().iter().filter(|name| **name == self.name).count();
        assert_eq!(count, 1, "the held action made {count} workspaces");
    }
}

/// What a start of build N+1 took.
struct Start {
    ready: Duration,
    catch_up: Duration,
    ensures: usize,
}

/// Starts build N+1 on `site` as the app does after an update and follows
/// it to a roster at `version` through `head`.
fn start_new(
    settings: &Settings,
    site: &Site,
    label: &str,
    version: u64,
    expected_entries: &Value,
) -> Start {
    let registry = site.registry();
    let head = journal_head(&registry);
    let mut owner = Owner::spawn(site, &settings.new_bin, label);
    let mut action = HeldAction::new(label);
    action.try_first(&owner);
    // The app's ensure runs once the new owner holds the socket (before, it
    // would start an owner of its own).
    let until = owner.started + settings.deadline;
    while !site.socket().exists() {
        owner.assert_alive("binding its socket");
        assert!(Instant::now() < until, "owner {label} never listened:\n{}", owner.log_tail());
        action.sweep(&owner, false);
        std::thread::sleep(Duration::from_millis(10));
    }
    let ensures = EnsureLoop::start(site, &settings.new_bin);
    let ready = loop {
        owner.assert_alive("restoring");
        let identity = owner.identify(Duration::from_secs(2));
        if identity.as_ref().is_some_and(|identity| identity["lifecycle_ready"] == true) {
            break owner.started.elapsed();
        }
        action.sweep(&owner, false);
        assert!(Instant::now() < until, "owner {label} not ready:\n{}", owner.log_tail());
        std::thread::sleep(Duration::from_millis(20));
    };
    action.sweep(&owner, true);
    eprintln!("e2e: {label}: ready in {:.2} s", ready.as_secs_f64());
    let catch_up = loop {
        owner.assert_alive("folding the roster");
        if let Some((saved, cursor, _)) = roster(&registry)
            && saved == version
            && cursor >= head
        {
            break owner.started.elapsed();
        }
        assert!(
            Instant::now() < until,
            "owner {label}: the roster never reached version {version} at {head}: {:?}\n{}",
            roster(&registry).map(|(saved, cursor, _)| (saved, cursor)),
            owner.log_tail()
        );
        std::thread::sleep(Duration::from_millis(250));
    };
    eprintln!("e2e: {label}: roster caught up in {:.2} s", catch_up.as_secs_f64());
    // A few more ensures after the catch-up, then the verdict.
    std::thread::sleep(ENSURE_EVERY * 2);
    owner.assert_alive("serving after its restore");
    let ensures = ensures.finish(owner.pid());
    assert!(ensures > 0, "no server ensure ran");
    action.assert_once(&owner);
    let (_, _, entries) = roster(&registry).unwrap();
    assert_eq!(&entries, expected_entries, "{label}: the re-folded roster differs from the fold");
    owner.hand_off(&settings.new_bin);
    Start { ready, catch_up, ensures }
}

/// Starts build N on `site`, checks it answers and its roster is current.
fn start_old<'a>(settings: &Settings, site: &'a Site, label: &str) -> Owner<'a> {
    let mut owner = Owner::spawn(site, &settings.old_bin, label);
    let ready = owner.wait_ready(settings.deadline);
    let names = owner.workspace_names();
    assert!(!names.is_empty(), "build N lost the workspaces");
    eprintln!("e2e: {label}: build N ready in {:.2} s", ready.as_secs_f64());
    owner
}

/// What build N+1's premigrate took while build N served and a client wrote.
fn premigrate(settings: &Settings, site: &Site, old: &mut Owner<'_>) -> Duration {
    let old_pid = old.pid();
    let stop = Arc::new(AtomicBool::new(false));
    let socket = site.socket();
    let prober = {
        let stop = stop.clone();
        std::thread::spawn(move || {
            let mut probes = 0_u32;
            let mut slowest = Duration::ZERO;
            while !stop.load(Ordering::Relaxed) {
                let asked = Instant::now();
                let identity = request(&socket, json!({"cmd": "identify"}), PROBE_BOUND)
                    .unwrap_or_else(|| panic!("build N did not answer probe {probes}"));
                assert_eq!(identity["data"]["pid"].as_u64(), Some(old_pid), "build N's pid changed");
                // The client keeps writing: one workspace per probe.
                let key = format!("e2e-live-{probes}");
                let created =
                    request(&socket, create_workspace(&format!("live {probes}"), &key), PROBE_BOUND)
                        .unwrap_or_else(|| panic!("build N did not answer write {probes}"));
                assert_eq!(created["ok"], true, "build N refused a write: {created}");
                slowest = slowest.max(asked.elapsed());
                probes += 1;
                std::thread::sleep(PROBE_EVERY.saturating_sub(asked.elapsed()));
            }
            (probes, slowest)
        })
    };
    let started = Instant::now();
    let output = site
        .command(&settings.new_bin)
        .args(["--session", SESSION, "registry", "premigrate", "--json"])
        .output()
        .unwrap();
    let took = started.elapsed();
    stop.store(true, Ordering::Relaxed);
    let probed = prober.join();
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "registry premigrate failed: {stdout}{stderr}");
    let (probes, slowest) = probed.unwrap_or_else(|_| panic!("build N stopped answering"));
    old.assert_alive("serving during the premigrate");
    assert_eq!(old.identify(PROBE_BOUND).and_then(|identity| identity["pid"].as_u64()), Some(old_pid));
    eprintln!(
        "e2e: premigrate: {:.2} s, build N answered {probes} probes (slowest {:.0} ms): {}",
        took.as_secs_f64(),
        slowest.as_secs_f64() * 1000.0,
        last_json(&stdout).unwrap_or(Value::Null)
    );
    assert!(probes > 0 || took < PROBE_EVERY, "build N was never probed");
    took
}

#[test]
#[ignore = "end to end: builds a registry of many GB; run by scripts/e2e/update-premigrate.sh"]
fn an_update_that_bumps_the_roster_reducer_repairs_itself() {
    let settings = Settings::from_env();
    fs::create_dir_all(&settings.root).unwrap();
    let records = settings.records;
    let agent_events = (records / 100).max(1);
    let output_rows = records / 10;
    let session_puts = records - agent_events - output_rows;

    let template = Site::new(&settings.root, "template");
    let generated = generate(
        &template.state_root(),
        &SynthOptions {
            session: SESSION.into(),
            session_puts,
            agent_events,
            output_bytes: output_rows * 4096,
            workspaces: 4,
            payload_bytes: settings.payload_bytes,
            seal: true,
            roster_version_bump: false,
            batch_rows: 20_000,
            progress: false,
        },
    )
    .unwrap();
    eprintln!("e2e: generated {}", generated.to_json());
    let old_version = u64::from(generated.roster_version);
    let new_version = settings.new_version.unwrap_or(old_version + 1);
    assert_ne!(new_version, old_version, "build N+1 must bump the roster reducer");
    let (_, _, entries) = roster(&template.registry()).unwrap();
    assert!(entries.as_object().is_some_and(|entries| !entries.is_empty()), "the fold has no agents");

    // Premigrate's copy first: the baseline runs on the template itself.
    let warm_site =
        (!settings.baseline_only).then(|| Site::copy_of(&template, &settings.root, "premigrate"));

    let old = start_old(&settings, &template, "baseline-n");
    old.hand_off(&settings.new_bin);
    let baseline = start_new(&settings, &template, "baseline-n1", new_version, &entries);
    drop(template);

    let warm = warm_site.map(|warm_site| {
        let mut old = start_old(&settings, &warm_site, "premigrate-n");
        let took = premigrate(&settings, &warm_site, &mut old);
        old.hand_off(&settings.new_bin);
        (took, start_new(&settings, &warm_site, "premigrate-n1", new_version, &entries))
    });

    let seconds = |value: Option<Duration>| {
        value.map_or_else(|| "skipped".to_string(), |value| format!("{:.2}", value.as_secs_f64()))
    };
    let summary = format!(
        "E2E-RESULT records={records} baseline_ready_s={} baseline_catchup_s={} premigrate_s={} \
         warm_ready_s={} warm_catchup_s={}",
        seconds(Some(baseline.ready)),
        seconds(Some(baseline.catch_up)),
        seconds(warm.as_ref().map(|(took, _)| *took)),
        seconds(warm.as_ref().map(|(_, start)| start.ready)),
        seconds(warm.as_ref().map(|(_, start)| start.catch_up)),
    );
    eprintln!(
        "e2e: registry {} bytes, journal head {}, {} sealed segments, roster {old_version} -> \
         {new_version}, server ensure ran {} + {} times",
        generated.registry_bytes,
        generated.journal_head,
        generated.segments,
        baseline.ensures,
        warm.as_ref().map_or(0, |(_, start)| start.ensures),
    );
    println!("{summary}");
    if let Some(path) = &settings.result_file {
        fs::write(path, format!("{summary}\n")).unwrap();
    }
}
