//! Simulator-neutral live inspection and control exposed as a localhost MCP server.
//!
//! The HTTP worker never owns a VPI handle. It sends backend-neutral requests
//! through [`rustdv_debug::DebugClient`]; the testbench services them by calling
//! [`run_debug_service`] on the simulator thread. The service composes
//! RustDV's stable ReadOnly service point and existing time triggers; it does
//! not create another scheduler. Held pauses use bounded wall-clock waits and
//! fail open on raw channel disconnect or pause-inactivity lease expiry.

pub use rustdv_mcp_fst::{
    TraceChange, TraceChangePage, TraceRecordingConfig, TraceRecordingStatus, TraceResponse,
    TraceSnapshot, TraceSummary, TraceValueAt,
};
use rustdv_mcp_fst::{TraceClient, TraceRequest, TraceSession};

use axum::Router;
use rmcp::{handler::server::wrapper::Parameters, schemars, tool, tool_router};
use rustdv::sim::handle::HandleBase;
use rustdv::{HandleChildren, HierarchyHandle, SimHandle};
use rustdv_debug::{
    ControlMode, DebugClient, DebugDirective, DebugError, DebugRequest, PredicateSpec, SignalInfo,
    SignalKind, SignalProvider, SignalValue, WatchSpec,
};
use serde::Deserialize;
use std::{
    net::{SocketAddr, TcpListener},
    ops::{Deref, DerefMut},
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

/// Both sides of the simulator-thread services used with FST recording.
#[derive(Clone)]
pub struct VerilatorDebugClient {
    debug: DebugClient,
    trace: TraceClient,
}

/// Client accepted by [`McpServer`]. A plain [`DebugClient`] is used when the
/// selected simulator has no all-signal trace backend.
pub struct McpClient {
    debug: DebugClient,
    trace: Option<TraceClient>,
}

impl From<DebugClient> for McpClient {
    fn from(debug: DebugClient) -> Self {
        Self { debug, trace: None }
    }
}

impl From<VerilatorDebugClient> for McpClient {
    fn from(client: VerilatorDebugClient) -> Self {
        Self {
            debug: client.debug,
            trace: Some(client.trace),
        }
    }
}

impl McpServer {
    pub fn start(
        client: impl Into<McpClient>,
        config: McpServerConfig,
    ) -> Result<Self, DebugError> {
        let client = client.into();
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
            .name("rustdv-mcp".into())
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
                        client: client.debug,
                        trace: client.trace,
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
    trace: Option<TraceClient>,
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

    fn trace_json(&self, request: TraceRequest) -> String {
        let result = self.trace_request(request);
        match result {
            Ok(response) => serde_json::to_string_pretty(&response)
                .unwrap_or_else(|error| format!(r#"{{"error":"{error}"}}"#)),
            Err(error) => serde_json::to_string_pretty(&error)
                .unwrap_or_else(|_| format!(r#"{{"error":"{error}"}}"#)),
        }
    }

    fn trace_request(&self, request: TraceRequest) -> Result<TraceResponse, DebugError> {
        let trace = self
            .trace
            .as_ref()
            .ok_or_else(|| DebugError::new("all-signal trace recording is unavailable"))?;
        // Trace requests use a separate backend channel. Touching
        // the neutral control session first makes active trace work renew the
        // held-pause lease exactly as live VPI requests do.
        let _ = self
            .client
            .request_timeout(DebugRequest::ControlStatus, self.request_timeout)?;
        trace.request_timeout(request, self.request_timeout)
    }

    fn recording_json(&self, trace: TraceRequest, fallback: DebugRequest) -> String {
        if self.trace.is_some() {
            self.trace_json(trace)
        } else {
            self.request_json(fallback)
        }
    }

    fn status_json(&self) -> String {
        let debug = match self
            .client
            .request_timeout(DebugRequest::Status, self.request_timeout)
        {
            Ok(response) => response,
            Err(error) => {
                return serde_json::to_string_pretty(&error)
                    .unwrap_or_else(|_| format!(r#"{{"error":"{error}"}}"#))
            }
        };
        if self.trace.is_none() {
            return serde_json::to_string_pretty(&debug)
                .unwrap_or_else(|error| format!(r#"{{"error":"{error}"}}"#));
        }
        let trace = match self.trace_request(TraceRequest::Summary) {
            Ok(TraceResponse::Summary(summary)) => summary,
            Ok(_) => return r#"{"error":"trace summary returned the wrong response"}"#.to_owned(),
            Err(error) => {
                return serde_json::to_string_pretty(&error)
                    .unwrap_or_else(|_| format!(r#"{{"error":"{error}"}}"#))
            }
        };
        let mut value = serde_json::to_value(debug)
            .unwrap_or_else(|error| serde_json::json!({"error": error.to_string()}));
        if let Some(status) = value
            .get_mut("value")
            .and_then(serde_json::Value::as_object_mut)
        {
            status.insert("recordings".to_owned(), serde_json::json!(trace.recordings));
            status.insert(
                "trace".to_owned(),
                serde_json::to_value(trace)
                    .unwrap_or_else(|error| serde_json::json!({"error": error.to_string()})),
            );
        }
        serde_json::to_string_pretty(&value)
            .unwrap_or_else(|error| format!(r#"{{"error":"{error}"}}"#))
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
    #[serde(default)]
    signals: Vec<String>,
    #[serde(default = "default_recording_capacity")]
    capacity: usize,
}

fn default_recording_capacity() -> usize {
    4096
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

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct RecordingHierarchyParameters {
    name: String,
    #[serde(default)]
    scope: String,
    #[serde(default = "default_depth")]
    max_depth: usize,
    #[serde(default = "default_max_results")]
    max_results: usize,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct RecordingValueParameters {
    name: String,
    signal: String,
    time_steps: u64,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct RecordingChangesParameters {
    name: String,
    signal: String,
    #[serde(default)]
    start_time_steps: Option<u64>,
    #[serde(default)]
    end_time_steps: Option<u64>,
    #[serde(default)]
    cursor: Option<u64>,
    #[serde(default = "default_recording_limit")]
    limit: usize,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct RecordingSnapshotParameters {
    name: String,
    #[serde(default)]
    scope: String,
    time_steps: u64,
    #[serde(default = "default_snapshot_signals")]
    max_signals: usize,
}

fn default_snapshot_signals() -> usize {
    256
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
        self.status_json()
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

    #[tool(description = "Read one live simulator signal as a binary string")]
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

    #[tool(
        description = "Start recording; a capable trace backend captures all retained signals, otherwise selected signals are sampled"
    )]
    fn start_recording(
        &self,
        Parameters(RecordingStartParameters {
            name,
            signals,
            capacity,
        }): Parameters<RecordingStartParameters>,
    ) -> String {
        self.recording_json(
            TraceRequest::Start {
                name: name.clone(),
                signals: signals.clone(),
                capacity,
            },
            DebugRequest::StartRecording {
                name,
                signals,
                capacity,
            },
        )
    }

    #[tool(description = "Return status and overflow counters for one recording")]
    fn recording_status(
        &self,
        Parameters(RecordingNameParameters { name }): Parameters<RecordingNameParameters>,
    ) -> String {
        self.recording_json(
            TraceRequest::Status { name: name.clone() },
            DebugRequest::RecordingStatus { name },
        )
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
        self.recording_json(
            TraceRequest::Get {
                name: name.clone(),
                cursor,
                limit,
            },
            DebugRequest::GetRecording {
                name,
                cursor,
                limit,
            },
        )
    }

    #[tool(description = "Stop sampling one recording while retaining its history")]
    fn stop_recording(
        &self,
        Parameters(RecordingNameParameters { name }): Parameters<RecordingNameParameters>,
    ) -> String {
        self.recording_json(
            TraceRequest::Stop { name: name.clone() },
            DebugRequest::StopRecording { name },
        )
    }

    #[tool(description = "Remove one recording and its retained history")]
    fn remove_recording(
        &self,
        Parameters(RecordingNameParameters { name }): Parameters<RecordingNameParameters>,
    ) -> String {
        self.recording_json(
            TraceRequest::Remove { name: name.clone() },
            DebugRequest::RemoveRecording { name },
        )
    }

    #[tool(description = "List scopes and signals retained in a trace recording")]
    fn recording_hierarchy(
        &self,
        Parameters(RecordingHierarchyParameters {
            name,
            scope,
            max_depth,
            max_results,
        }): Parameters<RecordingHierarchyParameters>,
    ) -> String {
        self.trace_json(TraceRequest::Hierarchy {
            name,
            scope,
            max_depth,
            max_results,
        })
    }

    #[tool(
        description = "Return one recorded signal's most recent value at or before an exact simulation time"
    )]
    fn recording_value_at(
        &self,
        Parameters(RecordingValueParameters {
            name,
            signal,
            time_steps,
        }): Parameters<RecordingValueParameters>,
    ) -> String {
        self.trace_json(TraceRequest::ValueAt {
            name,
            signal,
            time_steps,
        })
    }

    #[tool(
        description = "Return a bounded page of changes for one signal in a recorded time range"
    )]
    fn recording_changes(
        &self,
        Parameters(RecordingChangesParameters {
            name,
            signal,
            start_time_steps,
            end_time_steps,
            cursor,
            limit,
        }): Parameters<RecordingChangesParameters>,
    ) -> String {
        self.trace_json(TraceRequest::Changes {
            name,
            signal,
            start_time_steps,
            end_time_steps,
            cursor,
            limit,
        })
    }

    #[tool(
        description = "Return a bounded scope snapshot from a recording at an exact simulation time"
    )]
    fn recording_snapshot(
        &self,
        Parameters(RecordingSnapshotParameters {
            name,
            scope,
            time_steps,
            max_signals,
        }): Parameters<RecordingSnapshotParameters>,
    ) -> String {
        self.trace_json(TraceRequest::Snapshot {
            name,
            scope,
            time_steps,
            max_signals,
        })
    }

    #[tool(
        description = "Return the trace backend capability, active recording, retained bytes, and configured capture limits"
    )]
    fn recording_backend_status(&self) -> String {
        self.trace_json(TraceRequest::Summary)
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

/// Simulator-thread state for live VPI debug plus optional private FST
/// recording. Dereferencing exposes the backend-neutral [`DebugSession`].
pub struct VerilatorDebugSession<P> {
    debug: DebugSession<P>,
    trace: TraceSession,
}

/// The same testbench-facing session works with a basic VPI backend or an
/// instrumented simulator trace backend. The runner selects the simulator;
/// no simulator name or trace format appears in testbench control flow.
pub struct SimulatorDebugSession<P> {
    backend: SessionBackend<P>,
}

enum SessionBackend<P> {
    Live(Box<DebugSession<P>>),
    WithTrace(Box<VerilatorDebugSession<P>>),
}

impl<P> Deref for VerilatorDebugSession<P> {
    type Target = DebugSession<P>;

    fn deref(&self) -> &Self::Target {
        &self.debug
    }
}

impl<P> DerefMut for VerilatorDebugSession<P> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.debug
    }
}

impl Deref for VerilatorDebugClient {
    type Target = DebugClient;

    fn deref(&self) -> &Self::Target {
        &self.debug
    }
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

/// Service contract implemented by the basic session and trace-capable sessions.
pub trait DebugServiceSession {
    fn service_stable_point(&mut self, paused_wait: Duration) -> DebugDirective;
    fn service_controller_connected(&self) -> bool;
    fn service_control_status(&self) -> rustdv_debug::ControlStatus;
    fn service_next_deadline_steps(&self) -> Option<u64>;
    fn service_shutdown(&mut self);
}

impl<P: SignalProvider> DebugServiceSession for DebugSession<P> {
    fn service_stable_point(&mut self, paused_wait: Duration) -> DebugDirective {
        service_current_stable_point(self, paused_wait)
    }

    fn service_controller_connected(&self) -> bool {
        self.controller_connected()
    }

    fn service_control_status(&self) -> rustdv_debug::ControlStatus {
        self.control_status()
    }

    fn service_next_deadline_steps(&self) -> Option<u64> {
        None
    }

    fn service_shutdown(&mut self) {}
}

impl<P: SignalProvider> DebugServiceSession for VerilatorDebugSession<P> {
    fn service_stable_point(&mut self, paused_wait: Duration) -> DebugDirective {
        let time = rustdv::sim_time_steps();
        let mut directive = self.debug.poll(time);
        self.trace.poll(time);
        while directive == DebugDirective::Hold {
            directive = self.debug.poll_wait(time, paused_wait);
            self.trace.poll(time);
        }
        directive
    }

    fn service_controller_connected(&self) -> bool {
        self.debug.controller_connected()
    }

    fn service_control_status(&self) -> rustdv_debug::ControlStatus {
        self.debug.control_status()
    }

    fn service_next_deadline_steps(&self) -> Option<u64> {
        self.trace.next_deadline_steps()
    }

    fn service_shutdown(&mut self) {
        self.trace.stop_active(rustdv::sim_time_steps());
    }
}

impl<P: SignalProvider> DebugServiceSession for SimulatorDebugSession<P> {
    fn service_stable_point(&mut self, paused_wait: Duration) -> DebugDirective {
        match &mut self.backend {
            SessionBackend::Live(session) => session.service_stable_point(paused_wait),
            SessionBackend::WithTrace(session) => session.service_stable_point(paused_wait),
        }
    }

    fn service_controller_connected(&self) -> bool {
        match &self.backend {
            SessionBackend::Live(session) => session.service_controller_connected(),
            SessionBackend::WithTrace(session) => session.service_controller_connected(),
        }
    }

    fn service_control_status(&self) -> rustdv_debug::ControlStatus {
        match &self.backend {
            SessionBackend::Live(session) => session.service_control_status(),
            SessionBackend::WithTrace(session) => session.service_control_status(),
        }
    }

    fn service_next_deadline_steps(&self) -> Option<u64> {
        match &self.backend {
            SessionBackend::Live(session) => session.service_next_deadline_steps(),
            SessionBackend::WithTrace(session) => session.service_next_deadline_steps(),
        }
    }

    fn service_shutdown(&mut self) {
        match &mut self.backend {
            SessionBackend::Live(session) => session.service_shutdown(),
            SessionBackend::WithTrace(session) => session.service_shutdown(),
        }
    }
}

/// Service MCP requests only at settled RustDV ReadOnly points.
///
/// No scheduler is created here. Running waits compose RustDV's existing
/// `next_time_step` and `Timer`; pause holds the current ReadOnly callback and
/// uses bounded wall-clock waits. Raw channel disconnect fails open immediately;
/// an HTTP controller that vanishes is released by the pause inactivity lease.
pub async fn run_debug_service<S: DebugServiceSession>(session: &mut S) -> DebugServiceExit {
    const PAUSED_WAIT: Duration = Duration::from_millis(10);
    loop {
        let directive =
            if rustdv::sim::phase::current_phase() == rustdv::sim::phase::SimPhase::ReadOnly {
                // A RustDV trigger continuation may already be executing in the
                // settled ReadOnly callback. Reuse that stable point instead of
                // illegally awaiting a second ReadOnly callback from inside it.
                session.service_stable_point(PAUSED_WAIT)
            } else {
                rustdv::service_read_only(|| session.service_stable_point(PAUSED_WAIT)).await
            };
        if !session.service_controller_connected() {
            session.service_shutdown();
            return DebugServiceExit::ControllerDisconnected;
        }
        if directive == DebugDirective::Terminate {
            session.service_shutdown();
            return DebugServiceExit::Terminated;
        }

        let now = rustdv::sim_time_steps();
        let status = session.service_control_status();
        let control_deadline = match status.mode {
            ControlMode::RunUntilTime => status.target_time_steps,
            ControlMode::RunUntilPredicate => status.timeout_time_steps,
            ControlMode::Running | ControlMode::Paused | ControlMode::Terminated => None,
        };
        let trace_deadline = session.service_next_deadline_steps();
        let deadline = [control_deadline, trace_deadline]
            .into_iter()
            .flatten()
            .min();
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

/// Compatibility name for existing Verilator testbenches.
pub use run_debug_service as run_verilator_debug_service;
/// Compatibility name for the original service contract.
pub use DebugServiceSession as VerilatorServiceSession;

pub struct VpiSignalProvider {
    root: HierarchyHandle,
}

/// Compatibility alias for the original Verilator-specific provider name.
pub type VerilatorSignalProvider = VpiSignalProvider;

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

impl VpiSignalProvider {
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
                .map_err(|error| DebugError::new(error.to_string()))?;
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
                SimHandle::Hierarchy(hierarchy) => {
                    output.push(SignalInfo {
                        path: hierarchy.full_name(),
                        kind: SignalKind::Scope,
                        width: None,
                    });
                    if depth > 0 {
                        Self::collect(hierarchy, depth - 1, output);
                    }
                }
                SimHandle::Logic(logic) => output.push(SignalInfo {
                    path: logic.full_name(),
                    kind: SignalKind::Logic,
                    width: Some(logic.size()),
                }),
                _ => {}
            }
            if output.len() >= 4096 {
                break;
            }
        }
    }
}

impl SignalProvider for VpiSignalProvider {
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
            binary: signal
                .get_binstr()
                .map_err(|error| DebugError::new(error.to_string()))?,
        })
    }
}

pub fn verilator_debug_session(
    root: HierarchyHandle,
) -> (
    VerilatorDebugSession<VpiSignalProvider>,
    VerilatorDebugClient,
) {
    verilator_debug_session_with_configs(
        root,
        DebugSessionConfig::default(),
        TraceRecordingConfig::default(),
    )
    .expect("default Verilator debug configuration must be valid")
}

pub fn verilator_debug_session_with_config(
    root: HierarchyHandle,
    config: DebugSessionConfig,
) -> Result<
    (
        VerilatorDebugSession<VpiSignalProvider>,
        VerilatorDebugClient,
    ),
    DebugError,
> {
    verilator_debug_session_with_configs(root, config, TraceRecordingConfig::default())
}

pub fn verilator_debug_session_with_configs(
    root: HierarchyHandle,
    debug_config: DebugSessionConfig,
    trace_config: TraceRecordingConfig,
) -> Result<
    (
        VerilatorDebugSession<VpiSignalProvider>,
        VerilatorDebugClient,
    ),
    DebugError,
> {
    let (debug, debug_client) =
        DebugSession::with_config(VpiSignalProvider::new(root), debug_config)?;
    let (trace, trace_client) = TraceSession::new(trace_config)?;
    Ok((
        VerilatorDebugSession { debug, trace },
        VerilatorDebugClient {
            debug: debug_client,
            trace: trace_client,
        },
    ))
}

/// Construct a debug session for the active simulator without backend names
/// in the testbench. Hosts exporting the optional trace ABI use their trace
/// adapter; other VPI simulators retain selected-signal recording support.
pub fn simulator_debug_session(
    root: HierarchyHandle,
) -> (SimulatorDebugSession<VpiSignalProvider>, McpClient) {
    simulator_debug_session_with_configs(
        root,
        DebugSessionConfig::default(),
        TraceRecordingConfig::default(),
    )
    .expect("default simulator debug configuration must be valid")
}

/// Construct a simulator-neutral debug session with live-control limits.
pub fn simulator_debug_session_with_config(
    root: HierarchyHandle,
    debug_config: DebugSessionConfig,
) -> Result<(SimulatorDebugSession<VpiSignalProvider>, McpClient), DebugError> {
    simulator_debug_session_with_configs(root, debug_config, TraceRecordingConfig::default())
}

/// Construct a simulator-neutral debug session with backend recording limits.
/// The trace configuration applies only when the selected host has a trace
/// backend; otherwise the selected-signal `rustdv-debug` limits apply.
pub fn simulator_debug_session_with_configs(
    root: HierarchyHandle,
    debug_config: DebugSessionConfig,
    trace_config: TraceRecordingConfig,
) -> Result<(SimulatorDebugSession<VpiSignalProvider>, McpClient), DebugError> {
    let (debug, debug_client) =
        DebugSession::with_config(VpiSignalProvider::new(root), debug_config)?;
    if rustdv::sim::simulator_trace::host_present()
        && rustdv::sim::simulator_trace::host_format()
            == Some(rustdv::sim::simulator_trace::TraceFormat::Fst)
    {
        let (trace, trace_client) = TraceSession::new(trace_config)?;
        Ok((
            SimulatorDebugSession {
                backend: SessionBackend::WithTrace(Box::new(VerilatorDebugSession {
                    debug,
                    trace,
                })),
            },
            McpClient {
                debug: debug_client,
                trace: Some(trace_client),
            },
        ))
    } else {
        Ok((
            SimulatorDebugSession {
                backend: SessionBackend::Live(Box::new(debug)),
            },
            McpClient {
                debug: debug_client,
                trace: None,
            },
        ))
    }
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
