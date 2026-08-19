//! Verilator/VPI live inspection and control exposed as a localhost MCP server.
//!
//! The HTTP worker never owns a VPI handle. It sends backend-neutral requests
//! through [`rustdv_debug::DebugClient`]; the testbench services them by calling
//! [`run_verilator_debug_service`] on the simulator thread. The service composes
//! RustDV's stable ReadOnly service point and existing time triggers; it does
//! not create another scheduler. Held pauses use bounded wall-clock waits and
//! fail open on raw channel disconnect or pause-inactivity lease expiry.

use axum::Router;
use rmcp::{handler::server::wrapper::Parameters, schemars, tool, tool_router};
use rustdv::{AnyHandle, HierarchyHandle};
use rustdv_debug::{
    ControlMode, DebugClient, DebugDirective, DebugError, DebugRequest, PredicateSpec, SignalInfo,
    SignalKind, SignalProvider, SignalValue, WatchSpec,
};
use serde::Deserialize;
use std::{
    net::{SocketAddr, TcpListener},
    thread::{self, JoinHandle},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

pub use rustdv_debug::{
    ControlStatus, DebugSession, DebugSessionConfig, PauseReason, RecordingPage, RecordingSample,
    RecordingSpec, RecordingStatus, WatchStats,
};

#[derive(Clone, Debug)]
pub struct McpServerConfig {
    /// Loopback address used by the unauthenticated local debug server.
    pub bind: SocketAddr,
    pub request_timeout: Duration,
}

impl Default for McpServerConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:9393".parse().expect("static socket address"),
            request_timeout: Duration::from_secs(2),
        }
    }
}

pub struct McpServer {
    endpoint: String,
    cancellation: CancellationToken,
    worker: Option<JoinHandle<()>>,
}

impl McpServer {
    pub fn start(client: DebugClient, config: McpServerConfig) -> Result<Self, DebugError> {
        if !config.bind.ip().is_loopback() {
            return Err(DebugError::new(
                "MCP server bind address must be a loopback address",
            ));
        }
        let listener = TcpListener::bind(config.bind)
            .map_err(|error| DebugError::new(format!("cannot bind MCP server: {error}")))?;
        listener
            .set_nonblocking(true)
            .map_err(|error| DebugError::new(format!("cannot configure MCP listener: {error}")))?;
        let address = listener
            .local_addr()
            .map_err(|error| DebugError::new(format!("cannot read MCP address: {error}")))?;
        let endpoint = format!("http://{address}/mcp");
        let cancellation = CancellationToken::new();
        let worker_cancellation = cancellation.clone();
        let worker = thread::Builder::new()
            .name("rustdv-mcp-verilator".into())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()
                    .expect("build rustdv MCP runtime");
                runtime.block_on(async move {
                    use rmcp::transport::streamable_http_server::{
                        session::local::LocalSessionManager, StreamableHttpServerConfig,
                        StreamableHttpService,
                    };

                    let handler = DebugMcpHandler {
                        client,
                        request_timeout: config.request_timeout,
                    };
                    let service: StreamableHttpService<DebugMcpHandler, LocalSessionManager> =
                        StreamableHttpService::new(
                            move || Ok(handler.clone()),
                            Default::default(),
                            StreamableHttpServerConfig::default()
                                .with_legacy_session_mode(false)
                                .with_json_response(true)
                                .with_sse_keep_alive(None)
                                .with_cancellation_token(worker_cancellation.child_token()),
                        );
                    let router = Router::new().nest_service("/mcp", service);
                    let listener = tokio::net::TcpListener::from_std(listener)
                        .expect("convert rustdv MCP listener");
                    let _ = axum::serve(listener, router)
                        .with_graceful_shutdown(worker_cancellation.cancelled_owned())
                        .await;
                });
            })
            .map_err(|error| DebugError::new(format!("cannot start MCP worker: {error}")))?;

        Ok(Self {
            endpoint,
            cancellation,
            worker: Some(worker),
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

impl Drop for McpServer {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

#[derive(Clone)]
struct DebugMcpHandler {
    client: DebugClient,
    request_timeout: Duration,
}

impl DebugMcpHandler {
    fn request_json(&self, request: DebugRequest) -> String {
        match self.client.request_timeout(request, self.request_timeout) {
            Ok(response) => serde_json::to_string_pretty(&response)
                .unwrap_or_else(|error| format!(r#"{{"error":"{error}"}}"#)),
            Err(error) => serde_json::to_string_pretty(&error)
                .unwrap_or_else(|_| format!(r#"{{"error":"{error}"}}"#)),
        }
    }
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct HierarchyParameters {
    #[serde(default)]
    path: String,
    #[serde(default = "default_depth")]
    max_depth: usize,
    #[serde(default = "default_max_results")]
    max_results: usize,
}

fn default_depth() -> usize {
    1
}

fn default_max_results() -> usize {
    256
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct SignalParameters {
    path: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct WatchParameters {
    name: String,
    #[serde(default)]
    all_high: Vec<String>,
    #[serde(default)]
    all_low: Vec<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct WatchNameParameters {
    name: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct RecordingStartParameters {
    name: String,
    signals: Vec<String>,
    capacity: usize,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct RecordingNameParameters {
    name: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct RecordingGetParameters {
    name: String,
    #[serde(default)]
    cursor: Option<u64>,
    #[serde(default = "default_recording_limit")]
    limit: usize,
}

fn default_recording_limit() -> usize {
    256
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct RunUntilTimeParameters {
    target_time_steps: u64,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct RunUntilPredicateParameters {
    #[serde(default)]
    all_high: Vec<String>,
    #[serde(default)]
    all_low: Vec<String>,
    timeout_steps: u64,
}

#[tool_router(server_handler)]
impl DebugMcpHandler {
    #[tool(
        description = "Return live simulator time, poll count, DUT root, watch and recording counts, and control state"
    )]
    fn simulation_status(&self) -> String {
        self.request_json(DebugRequest::Status)
    }

    #[tool(description = "List scopes and signals below a DUT hierarchy path")]
    fn list_hierarchy(
        &self,
        Parameters(HierarchyParameters {
            path,
            max_depth,
            max_results,
        }): Parameters<HierarchyParameters>,
    ) -> String {
        self.request_json(DebugRequest::ListHierarchy {
            path,
            max_depth,
            max_results,
        })
    }

    #[tool(description = "Read one live Verilator signal as a four-state binary string")]
    fn read_signal(
        &self,
        Parameters(SignalParameters { path }): Parameters<SignalParameters>,
    ) -> String {
        self.request_json(DebugRequest::ReadSignal { path })
    }

    #[tool(
        description = "Count samples where every all_high signal is 1 and every all_low signal is 0"
    )]
    fn add_watch(
        &self,
        Parameters(WatchParameters {
            name,
            all_high,
            all_low,
        }): Parameters<WatchParameters>,
    ) -> String {
        self.request_json(DebugRequest::AddWatch {
            spec: WatchSpec {
                name,
                all_high,
                all_low,
            },
        })
    }

    #[tool(description = "Return sampled statistics for all active watches")]
    fn list_watches(&self) -> String {
        self.request_json(DebugRequest::ListWatches)
    }

    #[tool(description = "Remove a sampled watch by name")]
    fn remove_watch(
        &self,
        Parameters(WatchNameParameters { name }): Parameters<WatchNameParameters>,
    ) -> String {
        self.request_json(DebugRequest::RemoveWatch { name })
    }

    #[tool(description = "Start a bounded change-only recording of explicit signals")]
    fn start_recording(
        &self,
        Parameters(RecordingStartParameters {
            name,
            signals,
            capacity,
        }): Parameters<RecordingStartParameters>,
    ) -> String {
        self.request_json(DebugRequest::StartRecording {
            name,
            signals,
            capacity,
        })
    }

    #[tool(description = "Return status and overflow counters for one recording")]
    fn recording_status(
        &self,
        Parameters(RecordingNameParameters { name }): Parameters<RecordingNameParameters>,
    ) -> String {
        self.request_json(DebugRequest::RecordingStatus { name })
    }

    #[tool(description = "Read one bounded page of recording samples by monotonic cursor")]
    fn get_recording(
        &self,
        Parameters(RecordingGetParameters {
            name,
            cursor,
            limit,
        }): Parameters<RecordingGetParameters>,
    ) -> String {
        self.request_json(DebugRequest::GetRecording {
            name,
            cursor,
            limit,
        })
    }

    #[tool(description = "Stop sampling one recording while retaining its history")]
    fn stop_recording(
        &self,
        Parameters(RecordingNameParameters { name }): Parameters<RecordingNameParameters>,
    ) -> String {
        self.request_json(DebugRequest::StopRecording { name })
    }

    #[tool(description = "Remove one recording and its retained history")]
    fn remove_recording(
        &self,
        Parameters(RecordingNameParameters { name }): Parameters<RecordingNameParameters>,
    ) -> String {
        self.request_json(DebugRequest::RemoveRecording { name })
    }

    #[tool(description = "Return the current pause, run-until, or termination state")]
    fn control_status(&self) -> String {
        self.request_json(DebugRequest::ControlStatus)
    }

    #[tool(description = "Pause at the current settled ReadOnly stable point")]
    fn pause_simulation(&self) -> String {
        self.request_json(DebugRequest::Pause)
    }

    #[tool(description = "Resume normal simulation progress from a held stable point")]
    fn resume_simulation(&self) -> String {
        self.request_json(DebugRequest::Resume)
    }

    #[tool(description = "Arm an asynchronous exact absolute simulation-time stop")]
    fn run_until_time(
        &self,
        Parameters(RunUntilTimeParameters { target_time_steps }): Parameters<
            RunUntilTimeParameters,
        >,
    ) -> String {
        self.request_json(DebugRequest::RunUntilTime { target_time_steps })
    }

    #[tool(description = "Arm an asynchronous bounded all-high/all-low predicate stop")]
    fn run_until_predicate(
        &self,
        Parameters(RunUntilPredicateParameters {
            all_high,
            all_low,
            timeout_steps,
        }): Parameters<RunUntilPredicateParameters>,
    ) -> String {
        self.request_json(DebugRequest::RunUntilPredicate {
            predicate: PredicateSpec { all_high, all_low },
            timeout_steps,
        })
    }

    #[tool(description = "Terminate the opted-in debug service loop cleanly")]
    fn terminate_simulation(&self) -> String {
        self.request_json(DebugRequest::Terminate)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DebugServiceExit {
    Terminated,
    ControllerDisconnected,
}

fn service_current_stable_point<P: SignalProvider>(
    session: &mut DebugSession<P>,
    paused_wait: Duration,
) -> DebugDirective {
    let time = rustdv::sim_time_steps();
    let mut directive = session.poll(time);
    while directive == DebugDirective::Hold {
        directive = session.poll_wait(time, paused_wait);
    }
    directive
}

/// Service MCP requests only at settled RustDV ReadOnly points.
///
/// No scheduler is created here. Running waits compose RustDV's existing
/// `next_time_step` and `Timer`; pause holds the current ReadOnly callback and
/// uses bounded wall-clock waits. Raw channel disconnect fails open immediately;
/// an HTTP controller that vanishes is released by the pause inactivity lease.
pub async fn run_verilator_debug_service<P: SignalProvider>(
    session: &mut DebugSession<P>,
) -> DebugServiceExit {
    const PAUSED_WAIT: Duration = Duration::from_millis(10);
    loop {
        let directive = if rustdv::sim::phase::current_phase()
            == rustdv::sim::phase::SimPhase::ReadOnly
        {
            // A RustDV trigger continuation may already be executing in the
            // settled ReadOnly callback. Reuse that stable point instead of
            // illegally awaiting a second ReadOnly callback from inside it.
            service_current_stable_point(session, PAUSED_WAIT)
        } else {
            rustdv::service_read_only(|| service_current_stable_point(session, PAUSED_WAIT)).await
        };

        if !session.controller_connected() {
            return DebugServiceExit::ControllerDisconnected;
        }
        if directive == DebugDirective::Terminate {
            return DebugServiceExit::Terminated;
        }

        let now = rustdv::sim_time_steps();
        let status = session.control_status();
        let deadline = match status.mode {
            ControlMode::RunUntilTime => status.target_time_steps,
            ControlMode::RunUntilPredicate => status.timeout_time_steps,
            ControlMode::Running | ControlMode::Paused | ControlMode::Terminated => None,
        };
        if let Some(steps) = deadline
            .map(|deadline| deadline.saturating_sub(now))
            .filter(|steps| *steps > 0)
        {
            let _ = rustdv::first2(rustdv::next_time_step(), rustdv::Timer::steps(steps)).await;
        } else {
            rustdv::next_time_step().await;
        }
    }
}

pub struct VerilatorSignalProvider {
    root: HierarchyHandle,
}

fn validate_vpi_path(path: &str) -> Result<(), DebugError> {
    if path.contains('\0') {
        return Err(DebugError::new("VPI path cannot contain an interior NUL"));
    }
    Ok(())
}

fn normalize_scope_path(path: &str, root_name: &str, root_short_name: &str) -> Option<String> {
    fn strip_alias(value: &str, alias: &str) -> Option<Option<String>> {
        if alias.is_empty() {
            return None;
        }
        if value == alias {
            return Some(None);
        }
        value
            .strip_prefix(alias)
            .and_then(|rest| rest.strip_prefix('.'))
            .map(|rest| Some(rest.to_owned()))
    }

    let normalized = path.trim().trim_matches('.');
    if normalized.is_empty() || normalized == "TOP" {
        return None;
    }

    let root_without_top = root_name.strip_prefix("TOP.").unwrap_or(root_name);
    let aliases = [root_name, root_without_top, root_short_name];
    for alias in aliases {
        if let Some(relative) = strip_alias(normalized, alias) {
            return relative;
        }
    }

    if let Some(without_top) = normalized.strip_prefix("TOP.") {
        for alias in aliases {
            if let Some(relative) = strip_alias(without_top, alias) {
                return relative;
            }
        }
        return Some(without_top.to_owned());
    }

    Some(normalized.to_owned())
}

impl VerilatorSignalProvider {
    pub fn new(root: HierarchyHandle) -> Self {
        Self { root }
    }

    fn resolve_scope(&self, path: &str) -> Result<HierarchyHandle, DebugError> {
        validate_vpi_path(path)?;
        let root_name = self.root.full_name();
        let root_short_name = self.root.name();
        let Some(relative) = normalize_scope_path(path, &root_name, &root_short_name) else {
            return Ok(self.root);
        };
        let mut scope = self.root;
        for component in relative.split('.') {
            scope = scope
                .child(component)
                .map_err(|error| DebugError::new(error.to_string()))?
                .as_hierarchy()
                .ok_or_else(|| {
                    DebugError::new(format!("hierarchy component '{component}' is not a scope"))
                })?;
        }
        Ok(scope)
    }

    fn resolve_signal(&self, path: &str) -> Result<rustdv::LogicHandle, DebugError> {
        validate_vpi_path(path)?;
        let normalized = path.trim().trim_matches('.');
        let (scope, signal) = normalized
            .rsplit_once('.')
            .map_or(("", normalized), |(scope, signal)| (scope, signal));
        self.resolve_scope(scope)?
            .signal(signal)
            .map_err(|error| DebugError::new(error.to_string()))
    }

    fn collect(scope: HierarchyHandle, depth: usize, output: &mut Vec<SignalInfo>) {
        if output.len() >= 4096 {
            return;
        }
        for child in scope.children() {
            match child {
                AnyHandle::Hierarchy(hierarchy) => {
                    output.push(SignalInfo {
                        path: hierarchy.full_name(),
                        kind: SignalKind::Scope,
                        width: None,
                    });
                    if depth > 0 {
                        Self::collect(hierarchy, depth - 1, output);
                    }
                }
                AnyHandle::Logic(logic) => output.push(SignalInfo {
                    path: logic.full_name(),
                    kind: SignalKind::Logic,
                    width: Some(logic.size()),
                }),
                AnyHandle::Other => {}
            }
            if output.len() >= 4096 {
                break;
            }
        }
    }
}

impl SignalProvider for VerilatorSignalProvider {
    fn root_name(&self) -> String {
        self.root.full_name()
    }

    fn list_hierarchy(
        &mut self,
        path: &str,
        max_depth: usize,
    ) -> Result<Vec<SignalInfo>, DebugError> {
        let scope = self.resolve_scope(path)?;
        let mut output = Vec::new();
        Self::collect(scope, max_depth, &mut output);
        Ok(output)
    }

    fn read_signal(&mut self, path: &str) -> Result<SignalValue, DebugError> {
        let signal = self.resolve_signal(path)?;
        Ok(SignalValue {
            path: signal.full_name(),
            width: signal.size(),
            binary: signal.get_binstr(),
        })
    }
}

pub fn verilator_debug_session(
    root: HierarchyHandle,
) -> (DebugSession<VerilatorSignalProvider>, DebugClient) {
    DebugSession::new(VerilatorSignalProvider::new(root))
}

pub fn verilator_debug_session_with_config(
    root: HierarchyHandle,
    config: DebugSessionConfig,
) -> Result<(DebugSession<VerilatorSignalProvider>, DebugClient), DebugError> {
    DebugSession::with_config(VerilatorSignalProvider::new(root), config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustdv_debug::{DebugResponse, DebugSession, SignalProvider};
    use std::{collections::HashMap, sync::mpsc, time::Instant};

    struct MockProvider {
        values: HashMap<String, SignalValue>,
    }

    fn call_mcp_tool<P: SignalProvider>(
        session: &mut DebugSession<P>,
        endpoint: &str,
        simulation_time_steps: u64,
        name: &str,
        arguments: serde_json::Value,
    ) -> DebugResponse {
        let endpoint = endpoint.to_owned();
        let name = name.to_owned();
        let display_name = name.clone();
        let (done_tx, done_rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.block_on(async move {
                let body = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/call",
                    "params": {"name": name, "arguments": arguments}
                });
                let response: serde_json::Value = reqwest::Client::new()
                    .post(endpoint)
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json, text/event-stream")
                    .json(&body)
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                done_tx.send(response).unwrap();
            });
        });
        let deadline = Instant::now() + Duration::from_secs(3);
        let response = loop {
            session.poll_wait(simulation_time_steps, Duration::from_millis(2));
            if let Ok(response) = done_rx.try_recv() {
                break response;
            }
            assert!(Instant::now() < deadline, "MCP tool call timed out");
        };
        worker.join().unwrap();
        let text = response["result"]["content"][0]["text"]
            .as_str()
            .expect("tool response text");
        serde_json::from_str(text).unwrap_or_else(|error| {
            panic!("tool {display_name} returned non-response {text}: {error}")
        })
    }

    #[test]
    fn hierarchy_paths_returned_by_verilator_resolve_from_nested_roots() {
        assert_eq!(
            normalize_scope_path("TOP.dut.block", "TOP.dut", "dut"),
            Some("block".into())
        );
        assert_eq!(
            normalize_scope_path("TOP.dut.block", "dut", "dut"),
            Some("block".into())
        );
        assert_eq!(normalize_scope_path("TOP.dut", "TOP.dut", "dut"), None);
    }

    #[test]
    fn interior_nul_is_rejected_before_vpi_lookup() {
        let error = validate_vpi_path("dut.bad\0name").unwrap_err();
        assert_eq!(error.message, "VPI path cannot contain an interior NUL");
    }

    #[test]
    fn mcp_server_rejects_non_loopback_bind_addresses() {
        let provider = MockProvider {
            values: HashMap::new(),
        };
        let (_session, client) = DebugSession::new(provider);
        let Err(error) = McpServer::start(
            client,
            McpServerConfig {
                bind: "0.0.0.0:0".parse().unwrap(),
                request_timeout: Duration::from_secs(1),
            },
        ) else {
            panic!("non-loopback MCP bind was accepted");
        };

        assert_eq!(
            error.message,
            "MCP server bind address must be a loopback address"
        );
    }

    impl SignalProvider for MockProvider {
        fn root_name(&self) -> String {
            "dut".into()
        }

        fn list_hierarchy(
            &mut self,
            _path: &str,
            _max_depth: usize,
        ) -> Result<Vec<SignalInfo>, DebugError> {
            Ok(Vec::new())
        }

        fn read_signal(&mut self, path: &str) -> Result<SignalValue, DebugError> {
            self.values
                .get(path)
                .cloned()
                .ok_or_else(|| DebugError::new(format!("unknown signal {path}")))
        }
    }

    #[test]
    fn streamable_http_mcp_reads_through_simulator_thread() {
        let provider = MockProvider {
            values: HashMap::from([(
                "dut.ready".into(),
                SignalValue {
                    path: "dut.ready".into(),
                    width: 1,
                    binary: "1".into(),
                },
            )]),
        };
        let (mut session, client) = DebugSession::new(provider);
        let server = McpServer::start(
            client,
            McpServerConfig {
                bind: "127.0.0.1:0".parse().unwrap(),
                request_timeout: Duration::from_secs(1),
            },
        )
        .unwrap();
        let endpoint = server.endpoint().to_string();
        let (done_tx, done_rx) = mpsc::channel();
        let client_thread = thread::spawn(move || {
            let runtime = tokio::runtime::Runtime::new().unwrap();
            runtime.block_on(async move {
                let http = reqwest::Client::new();
                let body = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/call",
                    "params": {
                        "name": "read_signal",
                        "arguments": {"path": "dut.ready"}
                    }
                });
                let response: serde_json::Value = http
                    .post(endpoint)
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json, text/event-stream")
                    .json(&body)
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                done_tx.send(response).unwrap();
            });
        });
        let deadline = Instant::now() + Duration::from_secs(3);
        let response = loop {
            session.poll(42);
            if let Ok(response) = done_rx.try_recv() {
                break response;
            }
            assert!(Instant::now() < deadline, "MCP smoke timed out");
            thread::sleep(Duration::from_millis(1));
        };
        client_thread.join().unwrap();
        let text = response["result"]["content"][0]["text"]
            .as_str()
            .expect("tool response text");
        let decoded: DebugResponse = serde_json::from_str(text).unwrap();
        assert_eq!(
            decoded,
            DebugResponse::Signal(SignalValue {
                path: "dut.ready".into(),
                width: 1,
                binary: "1".into(),
            })
        );
    }

    #[test]
    fn mcp_exposes_every_recording_and_control_command() {
        let provider = MockProvider {
            values: HashMap::from([(
                "dut.ready".into(),
                SignalValue {
                    path: "dut.ready".into(),
                    width: 1,
                    binary: "1".into(),
                },
            )]),
        };
        let (mut session, client) = DebugSession::new(provider);
        let server = McpServer::start(
            client,
            McpServerConfig {
                bind: "127.0.0.1:0".parse().unwrap(),
                request_timeout: Duration::from_secs(1),
            },
        )
        .unwrap();
        let endpoint = server.endpoint();

        assert!(matches!(
            call_mcp_tool(
                &mut session,
                endpoint,
                0,
                "start_recording",
                serde_json::json!({
                    "name": "ready",
                    "signals": ["dut.ready"],
                    "capacity": 4
                }),
            ),
            DebugResponse::RecordingStatus(_)
        ));
        assert!(matches!(
            call_mcp_tool(
                &mut session,
                endpoint,
                0,
                "recording_status",
                serde_json::json!({"name": "ready"}),
            ),
            DebugResponse::RecordingStatus(_)
        ));
        assert!(matches!(
            call_mcp_tool(
                &mut session,
                endpoint,
                0,
                "get_recording",
                serde_json::json!({"name": "ready", "cursor": 0, "limit": 1}),
            ),
            DebugResponse::Recording(_)
        ));
        assert!(matches!(
            call_mcp_tool(
                &mut session,
                endpoint,
                0,
                "stop_recording",
                serde_json::json!({"name": "ready"}),
            ),
            DebugResponse::RecordingStatus(_)
        ));
        assert_eq!(
            call_mcp_tool(
                &mut session,
                endpoint,
                0,
                "remove_recording",
                serde_json::json!({"name": "ready"}),
            ),
            DebugResponse::Removed(true)
        );

        let DebugResponse::Control(paused) = call_mcp_tool(
            &mut session,
            endpoint,
            0,
            "pause_simulation",
            serde_json::json!({}),
        ) else {
            panic!("pause returned wrong response");
        };
        assert_eq!(paused.mode, ControlMode::Paused);
        assert!(matches!(
            call_mcp_tool(
                &mut session,
                endpoint,
                0,
                "control_status",
                serde_json::json!({}),
            ),
            DebugResponse::Control(_)
        ));
        let DebugResponse::Control(resumed) = call_mcp_tool(
            &mut session,
            endpoint,
            0,
            "resume_simulation",
            serde_json::json!({}),
        ) else {
            panic!("resume returned wrong response");
        };
        assert_eq!(resumed.mode, ControlMode::Running);
        let DebugResponse::Control(time_armed) = call_mcp_tool(
            &mut session,
            endpoint,
            0,
            "run_until_time",
            serde_json::json!({"target_time_steps": 10}),
        ) else {
            panic!("run-until time returned wrong response");
        };
        assert_eq!(time_armed.mode, ControlMode::RunUntilTime);
        let DebugResponse::Control(predicate_hit) = call_mcp_tool(
            &mut session,
            endpoint,
            0,
            "run_until_predicate",
            serde_json::json!({
                "all_high": ["dut.ready"],
                "all_low": [],
                "timeout_steps": 5
            }),
        ) else {
            panic!("run-until predicate returned wrong response");
        };
        assert_eq!(predicate_hit.mode, ControlMode::Paused);
        assert_eq!(
            call_mcp_tool(
                &mut session,
                endpoint,
                0,
                "terminate_simulation",
                serde_json::json!({}),
            ),
            DebugResponse::Control(ControlStatus {
                mode: ControlMode::Terminated,
                simulation_time_steps: 0,
                target_time_steps: None,
                predicate: None,
                timeout_time_steps: None,
                pause_reason: None,
                last_error: None,
            })
        );
    }

    #[test]
    fn abandoned_http_pause_lease_fails_open_and_time_can_advance() {
        let provider = MockProvider {
            values: HashMap::new(),
        };
        let (mut session, client) = DebugSession::with_config(
            provider,
            DebugSessionConfig {
                pause_inactivity_timeout: Duration::from_millis(20),
                ..DebugSessionConfig::default()
            },
        )
        .unwrap();
        let server = McpServer::start(
            client,
            McpServerConfig {
                bind: "127.0.0.1:0".parse().unwrap(),
                request_timeout: Duration::from_secs(1),
            },
        )
        .unwrap();

        let DebugResponse::Control(paused) = call_mcp_tool(
            &mut session,
            server.endpoint(),
            42,
            "pause_simulation",
            serde_json::json!({}),
        ) else {
            panic!("pause returned wrong response");
        };
        assert_eq!(paused.mode, ControlMode::Paused);

        while session.poll_wait(42, Duration::from_millis(5)) == DebugDirective::Hold {}
        assert_eq!(
            session.control_status().pause_reason,
            Some(PauseReason::LeaseExpired)
        );
        session.poll(43);
        assert_eq!(session.control_status().simulation_time_steps, 43);
    }
}
