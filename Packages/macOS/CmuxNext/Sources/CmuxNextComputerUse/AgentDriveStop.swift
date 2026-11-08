/// Stop from a workspace row's drive indicator (Lawrence 2026-11-07): ends the Computer Use session
/// the agent in one tab started, through the same host call as Stop Computer Use
/// (``ComputerUseSessionControlling/stop(_:)``); with no session attributed to it, the newest, as
/// the File menu's Stop does.
@MainActor
public struct AgentDriveStop {
    let sessions: any ComputerUseSessionControlling

    /// - Parameter sessions: The CUA host (`CuaComputerUseSessions` in the App, a fake in tests).
    public init(sessions: any ComputerUseSessionControlling) {
        self.sessions = sessions
    }

    /// Stops the Computer Use session started in one of `terminals` (the tab's host and public `term_…`
    /// ids, as `agent.terminal_id` names them), else the newest live one.
    ///
    /// - Returns: The stopped session's id; nil when no session is live.
    /// - Throws: ``ComputerUseSessionError`` when the host refuses or does not answer.
    @discardableResult
    public func stopScreen(terminals: Set<String>) async throws -> String? {
        let live = try await sessions.liveSessions()
        let own = live.filter { $0.terminalID.map(terminals.contains) == true }
        guard let session = (own.isEmpty ? live : own).max(by: { $0.lastActionAt < $1.lastActionAt }) else { return nil }
        try await sessions.stop(session.id)
        return session.id
    }
}
