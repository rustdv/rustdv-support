//! Backend-neutral live-debug control for RustDV simulations.
//!
//! Transport workers submit [`DebugRequest`] values through [`DebugClient`].
//! The simulator thread owns a [`DebugSession`] and is the only thread which
//! touches the backend [`SignalProvider`]. Active watches, recordings, and a
//! run-until predicate share one cached provider snapshot at each simulation
//! time; repeated polls at a held stable point never resample.
//!
//! Recordings are bounded change-only rings with an immediate initial sample,
//! simulation timestamps, monotonic cursors, and explicit overflow/truncation
//! reporting. Control requests arm pause, resume, exact absolute-time stops,
//! bounded predicates, or termination without blocking the transport worker.
//! A transport must use bounded waits while paused: simulation-time timeouts
//! cannot fire while a ReadOnly stable point is deliberately held. Successfully
//! serviced requests renew a configurable wall-clock pause lease; inactivity
//! releases the pause so a vanished transport cannot wedge simulation forever.

use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    fmt,
    sync::{
        atomic::{AtomicU8, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, Sender, SyncSender, TryRecvError, TrySendError},
        Arc,
    },
    time::{Duration, Instant},
};

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_REQUEST_QUEUE_CAPACITY: usize = 128;
const DEFAULT_MAX_REQUESTS_PER_POLL: usize = 32;
const DEFAULT_MAX_WATCHES: usize = 64;
const DEFAULT_MAX_SIGNALS_PER_WATCH: usize = 64;
const DEFAULT_MAX_RECORDINGS: usize = 16;
const DEFAULT_MAX_SIGNALS_PER_RECORDING: usize = 64;
const DEFAULT_MAX_RECORDING_CAPACITY: usize = 4096;
const DEFAULT_MAX_RECORDING_PAGE_SIZE: usize = 256;
const DEFAULT_MAX_HIERARCHY_RESULTS: usize = 4096;
const DEFAULT_PAUSE_INACTIVITY_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalInfo {
    pub path: String,
    pub kind: SignalKind,
    pub width: Option<u32>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalKind {
    Scope,
    Logic,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalValue {
    pub path: String,
    pub width: u32,
    pub binary: String,
}

impl SignalValue {
    pub fn is_high(&self) -> bool {
        !self.binary.is_empty() && self.binary.bytes().all(|bit| bit == b'1')
    }

    pub fn is_low(&self) -> bool {
        !self.binary.is_empty() && self.binary.bytes().all(|bit| bit == b'0')
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchSpec {
    pub name: String,
    #[serde(default)]
    pub all_high: Vec<String>,
    #[serde(default)]
    pub all_low: Vec<String>,
}

impl WatchSpec {
    pub fn validate(&self) -> Result<(), DebugError> {
        if self.name.trim().is_empty() {
            return Err(DebugError::new("watch name cannot be empty"));
        }
        if self.all_high.is_empty() && self.all_low.is_empty() {
            return Err(DebugError::new(
                "watch must contain at least one all_high or all_low signal",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchStats {
    pub spec: WatchSpec,
    /// Complete watch evaluations. Failed signal reads are excluded.
    pub samples: u64,
    pub hits: u64,
    /// Watch evaluations which could not read every required signal.
    #[serde(default)]
    pub read_errors: u64,
    /// Most recent signal-read error, if any.
    #[serde(default)]
    pub last_error: Option<String>,
    pub current_run: u64,
    pub longest_run: u64,
    pub first_hit_time_steps: Option<u64>,
    pub last_hit_time_steps: Option<u64>,
}

impl WatchStats {
    fn new(spec: WatchSpec) -> Self {
        Self {
            spec,
            samples: 0,
            hits: 0,
            read_errors: 0,
            last_error: None,
            current_run: 0,
            longest_run: 0,
            first_hit_time_steps: None,
            last_hit_time_steps: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordingSpec {
    pub name: String,
    pub signals: Vec<String>,
    pub capacity: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordingSample {
    pub cursor: u64,
    pub simulation_time_steps: u64,
    pub values: Vec<SignalValue>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordingStatus {
    pub spec: RecordingSpec,
    pub active: bool,
    pub retained: usize,
    pub dropped: u64,
    pub next_cursor: u64,
    pub read_errors: u64,
    pub last_error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordingPage {
    pub name: String,
    pub samples: Vec<RecordingSample>,
    pub next_cursor: Option<u64>,
    pub dropped: u64,
    pub truncated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PredicateSpec {
    #[serde(default)]
    pub all_high: Vec<String>,
    #[serde(default)]
    pub all_low: Vec<String>,
}

impl PredicateSpec {
    fn validate(&self) -> Result<(), DebugError> {
        if self.all_high.is_empty() && self.all_low.is_empty() {
            return Err(DebugError::new(
                "predicate must contain at least one all_high or all_low signal",
            ));
        }
        if self
            .all_high
            .iter()
            .chain(&self.all_low)
            .any(|path| path.trim().is_empty())
        {
            return Err(DebugError::new("predicate signal path cannot be empty"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlMode {
    Running,
    Paused,
    RunUntilTime,
    RunUntilPredicate,
    Terminated,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PauseReason {
    Requested,
    TargetReached,
    PredicateMatched,
    PredicateTimeout,
    PredicateReadError,
    DeadlineMissed,
    LeaseExpired,
    ControllerDisconnected,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ControlStatus {
    pub mode: ControlMode,
    pub simulation_time_steps: u64,
    pub target_time_steps: Option<u64>,
    pub predicate: Option<PredicateSpec>,
    pub timeout_time_steps: Option<u64>,
    pub pause_reason: Option<PauseReason>,
    pub last_error: Option<String>,
}

impl ControlStatus {
    fn running(simulation_time_steps: u64) -> Self {
        Self {
            mode: ControlMode::Running,
            simulation_time_steps,
            target_time_steps: None,
            predicate: None,
            timeout_time_steps: None,
            pause_reason: None,
            last_error: None,
        }
    }
}

struct Recording {
    spec: RecordingSpec,
    active: bool,
    samples: VecDeque<RecordingSample>,
    next_cursor: u64,
    dropped: u64,
    read_errors: u64,
    last_error: Option<String>,
}

impl Recording {
    fn status(&self) -> RecordingStatus {
        RecordingStatus {
            spec: self.spec.clone(),
            active: self.active,
            retained: self.samples.len(),
            dropped: self.dropped,
            next_cursor: self.next_cursor,
            read_errors: self.read_errors,
            last_error: self.last_error.clone(),
        }
    }

    fn append(&mut self, simulation_time_steps: u64, values: Vec<SignalValue>) {
        if self
            .samples
            .back()
            .is_some_and(|previous| previous.values == values)
        {
            return;
        }
        if self.samples.len() == self.spec.capacity {
            self.samples.pop_front();
            self.dropped = self.dropped.saturating_add(1);
        }
        self.samples.push_back(RecordingSample {
            cursor: self.next_cursor,
            simulation_time_steps,
            values,
        });
        self.next_cursor = self.next_cursor.saturating_add(1);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DebugStatus {
    pub root: String,
    pub polls: u64,
    pub simulation_time_steps: u64,
    pub watches: usize,
    pub recordings: usize,
    pub control: ControlStatus,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "request", rename_all = "snake_case")]
pub enum DebugRequest {
    Status,
    ListHierarchy {
        path: String,
        max_depth: usize,
        max_results: usize,
    },
    ReadSignal {
        path: String,
    },
    AddWatch {
        spec: WatchSpec,
    },
    ListWatches,
    RemoveWatch {
        name: String,
    },
    StartRecording {
        name: String,
        signals: Vec<String>,
        capacity: usize,
    },
    RecordingStatus {
        name: String,
    },
    GetRecording {
        name: String,
        cursor: Option<u64>,
        limit: usize,
    },
    StopRecording {
        name: String,
    },
    RemoveRecording {
        name: String,
    },
    ControlStatus,
    Pause,
    Resume,
    RunUntilTime {
        target_time_steps: u64,
    },
    RunUntilPredicate {
        predicate: PredicateSpec,
        timeout_steps: u64,
    },
    Terminate,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "response", content = "value", rename_all = "snake_case")]
pub enum DebugResponse {
    Status(DebugStatus),
    Hierarchy(Vec<SignalInfo>),
    Signal(SignalValue),
    Watch(WatchStats),
    Watches(Vec<WatchStats>),
    RecordingStatus(RecordingStatus),
    Recording(RecordingPage),
    Control(ControlStatus),
    Removed(bool),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DebugError {
    pub message: String,
}

impl DebugError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for DebugError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for DebugError {}

pub trait SignalProvider {
    fn root_name(&self) -> String;
    fn list_hierarchy(
        &mut self,
        path: &str,
        max_depth: usize,
    ) -> Result<Vec<SignalInfo>, DebugError>;
    fn read_signal(&mut self, path: &str) -> Result<SignalValue, DebugError>;
}

struct RequestEnvelope {
    request: DebugRequest,
    response: Sender<Result<DebugResponse, DebugError>>,
    deadline: Instant,
    state: Arc<AtomicU8>,
}

const REQUEST_PENDING: u8 = 0;
const REQUEST_CLAIMED: u8 = 1;
const REQUEST_CANCELLED: u8 = 2;

#[derive(Clone)]
pub struct DebugClient {
    requests: SyncSender<RequestEnvelope>,
}

impl DebugClient {
    pub fn request(&self, request: DebugRequest) -> Result<DebugResponse, DebugError> {
        self.request_timeout(request, DEFAULT_REQUEST_TIMEOUT)
    }

    pub fn request_timeout(
        &self,
        request: DebugRequest,
        timeout: Duration,
    ) -> Result<DebugResponse, DebugError> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| DebugError::new("debug request timeout is too large"))?;
        let state = Arc::new(AtomicU8::new(REQUEST_PENDING));
        let (response_tx, response_rx) = mpsc::channel();
        self.requests
            .try_send(RequestEnvelope {
                request,
                response: response_tx,
                deadline,
                state: state.clone(),
            })
            .map_err(|error| match error {
                TrySendError::Full(_) => DebugError::new("debug request queue is full"),
                TrySendError::Disconnected(_) => {
                    DebugError::new("debug session is no longer running")
                }
            })?;
        match response_rx.recv_timeout(timeout) {
            Ok(result) => result,
            Err(RecvTimeoutError::Timeout) => {
                match state.compare_exchange(
                    REQUEST_PENDING,
                    REQUEST_CANCELLED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) | Err(REQUEST_CANCELLED) => Err(DebugError::new(
                        "debug request timed out waiting for simulator poll",
                    )),
                    Err(REQUEST_CLAIMED) => response_rx.recv().unwrap_or_else(|_| {
                        Err(DebugError::new(
                            "debug session stopped while executing a claimed request",
                        ))
                    }),
                    Err(state) => Err(DebugError::new(format!(
                        "debug request entered invalid state {state}"
                    ))),
                }
            }
            Err(RecvTimeoutError::Disconnected) => Err(DebugError::new(
                "debug session stopped before returning a response",
            )),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DebugDirective {
    Advance,
    Hold,
    Terminate,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DebugSessionConfig {
    /// Maximum queued requests awaiting a simulator-thread poll.
    pub request_queue_capacity: usize,
    /// Maximum queued requests serviced by one simulator-thread poll.
    pub max_requests_per_poll: usize,
    /// Maximum simultaneously active watches.
    pub max_watches: usize,
    /// Maximum explicit signal paths in one watch.
    pub max_signals_per_watch: usize,
    /// Maximum simultaneously retained recordings.
    pub max_recordings: usize,
    /// Maximum explicit signal paths in one recording.
    pub max_signals_per_recording: usize,
    /// Maximum ring-buffer capacity of one recording.
    pub max_recording_capacity: usize,
    /// Maximum samples returned by one recording request.
    pub max_recording_page_size: usize,
    /// Maximum hierarchy entries returned by one request.
    pub max_hierarchy_results: usize,
    /// Wall-clock inactivity allowed while a paused stable point is held.
    pub pause_inactivity_timeout: Duration,
}

impl Default for DebugSessionConfig {
    fn default() -> Self {
        Self {
            request_queue_capacity: DEFAULT_REQUEST_QUEUE_CAPACITY,
            max_requests_per_poll: DEFAULT_MAX_REQUESTS_PER_POLL,
            max_watches: DEFAULT_MAX_WATCHES,
            max_signals_per_watch: DEFAULT_MAX_SIGNALS_PER_WATCH,
            max_recordings: DEFAULT_MAX_RECORDINGS,
            max_signals_per_recording: DEFAULT_MAX_SIGNALS_PER_RECORDING,
            max_recording_capacity: DEFAULT_MAX_RECORDING_CAPACITY,
            max_recording_page_size: DEFAULT_MAX_RECORDING_PAGE_SIZE,
            max_hierarchy_results: DEFAULT_MAX_HIERARCHY_RESULTS,
            pause_inactivity_timeout: DEFAULT_PAUSE_INACTIVITY_TIMEOUT,
        }
    }
}

pub struct DebugSession<P> {
    provider: P,
    requests: Receiver<RequestEnvelope>,
    watches: BTreeMap<String, WatchStats>,
    recordings: BTreeMap<String, Recording>,
    polls: u64,
    simulation_time_steps: u64,
    max_requests_per_poll: usize,
    config: DebugSessionConfig,
    sampled_time_steps: Option<u64>,
    snapshot_time_steps: Option<u64>,
    snapshot: HashMap<String, Result<SignalValue, DebugError>>,
    control: ControlStatus,
    controller_connected: bool,
    last_request_activity: Instant,
}

impl<P: SignalProvider> DebugSession<P> {
    pub fn new(provider: P) -> (Self, DebugClient) {
        Self::with_config(provider, DebugSessionConfig::default())
            .expect("default debug session configuration must be valid")
    }

    pub fn with_config(
        provider: P,
        config: DebugSessionConfig,
    ) -> Result<(Self, DebugClient), DebugError> {
        if config.request_queue_capacity == 0 {
            return Err(DebugError::new(
                "debug request queue capacity must be greater than zero",
            ));
        }
        if config.max_requests_per_poll == 0 {
            return Err(DebugError::new(
                "maximum debug requests per poll must be greater than zero",
            ));
        }
        if config.max_watches == 0
            || config.max_signals_per_watch == 0
            || config.max_recordings == 0
            || config.max_signals_per_recording == 0
            || config.max_recording_capacity == 0
            || config.max_recording_page_size == 0
            || config.max_hierarchy_results == 0
        {
            return Err(DebugError::new(
                "debug session watch, recording, and response limits must be greater than zero",
            ));
        }
        if config.pause_inactivity_timeout.is_zero() {
            return Err(DebugError::new(
                "pause inactivity timeout must be greater than zero",
            ));
        }

        let (request_tx, request_rx) = mpsc::sync_channel(config.request_queue_capacity);
        Ok((
            Self {
                provider,
                requests: request_rx,
                watches: BTreeMap::new(),
                recordings: BTreeMap::new(),
                polls: 0,
                simulation_time_steps: 0,
                max_requests_per_poll: config.max_requests_per_poll,
                config,
                sampled_time_steps: None,
                snapshot_time_steps: None,
                snapshot: HashMap::new(),
                control: ControlStatus::running(0),
                controller_connected: true,
                last_request_activity: Instant::now(),
            },
            DebugClient {
                requests: request_tx,
            },
        ))
    }

    /// Sample every active watch and service a bounded number of requests.
    /// This method must be called only from the simulator thread.
    pub fn poll(&mut self, simulation_time_steps: u64) -> DebugDirective {
        self.begin_poll(simulation_time_steps);
        self.service_ready_requests(None);
        self.directive()
    }

    /// Wait for at most `wall_timeout` for one transport request, then service
    /// that request and the remaining per-poll budget. This is the bounded
    /// wait used while a RustDV ReadOnly stable point is held.
    pub fn poll_wait(
        &mut self,
        simulation_time_steps: u64,
        wall_timeout: Duration,
    ) -> DebugDirective {
        self.begin_poll(simulation_time_steps);
        match self.requests.recv_timeout(wall_timeout) {
            Ok(envelope) => self.service_ready_requests(Some(envelope)),
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => self.fail_open_disconnected(),
        }
        if self.control.mode == ControlMode::Paused
            && self.last_request_activity.elapsed() >= self.config.pause_inactivity_timeout
        {
            self.fail_open_lease_expired();
        }
        self.directive()
    }

    pub fn controller_connected(&self) -> bool {
        self.controller_connected
    }

    fn begin_poll(&mut self, simulation_time_steps: u64) {
        self.polls = self.polls.saturating_add(1);
        self.simulation_time_steps = simulation_time_steps;
        self.control.simulation_time_steps = simulation_time_steps;
        self.prepare_snapshot_time();
        if self.sampled_time_steps != Some(simulation_time_steps) {
            self.sample_active_state();
            self.sampled_time_steps = Some(simulation_time_steps);
        }
    }

    fn service_ready_requests(&mut self, first: Option<RequestEnvelope>) {
        let mut first = first;
        for _ in 0..self.max_requests_per_poll {
            let envelope = match first.take() {
                Some(envelope) => envelope,
                None => match self.requests.try_recv() {
                    Ok(envelope) => envelope,
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        self.fail_open_disconnected();
                        break;
                    }
                },
            };
            self.service_envelope(envelope);
        }
    }

    fn service_envelope(&mut self, envelope: RequestEnvelope) {
        if Instant::now() >= envelope.deadline {
            let _ = envelope.state.compare_exchange(
                REQUEST_PENDING,
                REQUEST_CANCELLED,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
            return;
        }
        if envelope
            .state
            .compare_exchange(
                REQUEST_PENDING,
                REQUEST_CLAIMED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return;
        }
        let result = self.handle(envelope.request);
        self.last_request_activity = Instant::now();
        let _ = envelope.response.send(result);
    }

    fn directive(&self) -> DebugDirective {
        match self.control.mode {
            ControlMode::Paused => DebugDirective::Hold,
            ControlMode::Terminated => DebugDirective::Terminate,
            ControlMode::Running | ControlMode::RunUntilTime | ControlMode::RunUntilPredicate => {
                DebugDirective::Advance
            }
        }
    }

    fn fail_open_disconnected(&mut self) {
        self.controller_connected = false;
        if self.control.mode == ControlMode::Paused {
            self.control = ControlStatus::running(self.simulation_time_steps);
            self.control.pause_reason = Some(PauseReason::ControllerDisconnected);
        }
    }

    fn fail_open_lease_expired(&mut self) {
        self.control = ControlStatus::running(self.simulation_time_steps);
        self.control.pause_reason = Some(PauseReason::LeaseExpired);
    }

    fn handle(&mut self, request: DebugRequest) -> Result<DebugResponse, DebugError> {
        if self.control.mode == ControlMode::Terminated
            && !matches!(
                request,
                DebugRequest::Status | DebugRequest::ControlStatus | DebugRequest::Terminate
            )
        {
            return Err(DebugError::new("debug session is terminated"));
        }
        match request {
            DebugRequest::Status => Ok(DebugResponse::Status(DebugStatus {
                root: self.provider.root_name(),
                polls: self.polls,
                simulation_time_steps: self.simulation_time_steps,
                watches: self.watches.len(),
                recordings: self.recordings.len(),
                control: self.control.clone(),
            })),
            DebugRequest::ListHierarchy {
                path,
                max_depth,
                max_results,
            } => {
                let mut entries = self.provider.list_hierarchy(&path, max_depth.min(8))?;
                entries.truncate(max_results.clamp(1, self.config.max_hierarchy_results));
                Ok(DebugResponse::Hierarchy(entries))
            }
            DebugRequest::ReadSignal { path } => self.read_cached(&path).map(DebugResponse::Signal),
            DebugRequest::AddWatch { spec } => {
                spec.validate()?;
                let signal_count = spec.all_high.len().saturating_add(spec.all_low.len());
                if signal_count > self.config.max_signals_per_watch {
                    return Err(DebugError::new(format!(
                        "watch must select between 1 and {} signals",
                        self.config.max_signals_per_watch
                    )));
                }
                if !self.watches.contains_key(&spec.name)
                    && self.watches.len() >= self.config.max_watches
                {
                    return Err(DebugError::new("maximum watch count reached"));
                }
                for path in spec.all_high.iter().chain(&spec.all_low) {
                    self.read_cached(path)?;
                }
                let stats = WatchStats::new(spec);
                self.watches.insert(stats.spec.name.clone(), stats.clone());
                Ok(DebugResponse::Watch(stats))
            }
            DebugRequest::ListWatches => Ok(DebugResponse::Watches(
                self.watches.values().cloned().collect(),
            )),
            DebugRequest::RemoveWatch { name } => {
                Ok(DebugResponse::Removed(self.watches.remove(&name).is_some()))
            }
            DebugRequest::StartRecording {
                name,
                signals,
                capacity,
            } => self.start_recording(name, signals, capacity),
            DebugRequest::RecordingStatus { name } => self
                .recordings
                .get(&name)
                .map(|recording| DebugResponse::RecordingStatus(recording.status()))
                .ok_or_else(|| DebugError::new(format!("recording '{name}' does not exist"))),
            DebugRequest::GetRecording {
                name,
                cursor,
                limit,
            } => self.get_recording(&name, cursor, limit),
            DebugRequest::StopRecording { name } => {
                let recording = self
                    .recordings
                    .get_mut(&name)
                    .ok_or_else(|| DebugError::new(format!("recording '{name}' does not exist")))?;
                recording.active = false;
                Ok(DebugResponse::RecordingStatus(recording.status()))
            }
            DebugRequest::RemoveRecording { name } => Ok(DebugResponse::Removed(
                self.recordings.remove(&name).is_some(),
            )),
            DebugRequest::ControlStatus => Ok(DebugResponse::Control(self.control_status())),
            DebugRequest::Pause => {
                self.pause(PauseReason::Requested);
                Ok(DebugResponse::Control(self.control_status()))
            }
            DebugRequest::Resume => {
                self.control = ControlStatus::running(self.simulation_time_steps);
                Ok(DebugResponse::Control(self.control_status()))
            }
            DebugRequest::RunUntilTime { target_time_steps } => {
                if target_time_steps < self.simulation_time_steps {
                    return Err(DebugError::new(format!(
                        "run-until target {target_time_steps} is before current simulation time {}",
                        self.simulation_time_steps
                    )));
                }
                if target_time_steps == self.simulation_time_steps {
                    self.control = ControlStatus {
                        mode: ControlMode::Paused,
                        simulation_time_steps: self.simulation_time_steps,
                        target_time_steps: Some(target_time_steps),
                        predicate: None,
                        timeout_time_steps: None,
                        pause_reason: Some(PauseReason::TargetReached),
                        last_error: None,
                    };
                } else {
                    self.control = ControlStatus {
                        mode: ControlMode::RunUntilTime,
                        simulation_time_steps: self.simulation_time_steps,
                        target_time_steps: Some(target_time_steps),
                        predicate: None,
                        timeout_time_steps: None,
                        pause_reason: None,
                        last_error: None,
                    };
                }
                Ok(DebugResponse::Control(self.control_status()))
            }
            DebugRequest::RunUntilPredicate {
                predicate,
                timeout_steps,
            } => self.arm_run_until_predicate(predicate, timeout_steps),
            DebugRequest::Terminate => {
                self.control = ControlStatus {
                    mode: ControlMode::Terminated,
                    simulation_time_steps: self.simulation_time_steps,
                    target_time_steps: None,
                    predicate: None,
                    timeout_time_steps: None,
                    pause_reason: None,
                    last_error: None,
                };
                Ok(DebugResponse::Control(self.control_status()))
            }
        }
    }

    pub fn control_status(&self) -> ControlStatus {
        self.control.clone()
    }

    fn pause(&mut self, reason: PauseReason) {
        self.last_request_activity = Instant::now();
        self.control = ControlStatus {
            mode: ControlMode::Paused,
            simulation_time_steps: self.simulation_time_steps,
            target_time_steps: self.control.target_time_steps,
            predicate: self.control.predicate.clone(),
            timeout_time_steps: self.control.timeout_time_steps,
            pause_reason: Some(reason),
            last_error: self.control.last_error.clone(),
        };
    }

    fn prepare_snapshot_time(&mut self) {
        if self.snapshot_time_steps != Some(self.simulation_time_steps) {
            self.snapshot_time_steps = Some(self.simulation_time_steps);
            self.snapshot.clear();
        }
    }

    fn read_cached(&mut self, path: &str) -> Result<SignalValue, DebugError> {
        self.prepare_snapshot_time();
        if let Some(value) = self.snapshot.get(path) {
            return value.clone();
        }
        let value = self.provider.read_signal(path);
        self.snapshot.insert(path.to_owned(), value.clone());
        value
    }

    fn snapshot_paths(&mut self, paths: impl IntoIterator<Item = String>) {
        for path in paths {
            let _ = self.read_cached(&path);
        }
    }

    fn sample_active_state(&mut self) {
        let mut paths = BTreeSet::new();
        for stats in self.watches.values() {
            paths.extend(stats.spec.all_high.iter().cloned());
            paths.extend(stats.spec.all_low.iter().cloned());
        }
        for recording in self
            .recordings
            .values()
            .filter(|recording| recording.active)
        {
            paths.extend(recording.spec.signals.iter().cloned());
        }
        if self.control.mode == ControlMode::RunUntilPredicate {
            if let Some(predicate) = &self.control.predicate {
                paths.extend(predicate.all_high.iter().cloned());
                paths.extend(predicate.all_low.iter().cloned());
            }
        }
        self.snapshot_paths(paths);
        self.sample_watches();
        self.sample_recordings();
        self.update_control_after_sample();
    }

    fn sample_watches(&mut self) {
        let snapshot = &self.snapshot;
        for stats in self.watches.values_mut() {
            let sample: Result<bool, DebugError> =
                (|| {
                    let mut hit = true;
                    for path in &stats.spec.all_high {
                        let value = snapshot.get(path).cloned().unwrap_or_else(|| {
                            Err(DebugError::new(format!("no sample for {path}")))
                        })?;
                        hit = hit && value.is_high();
                    }
                    for path in &stats.spec.all_low {
                        let value = snapshot.get(path).cloned().unwrap_or_else(|| {
                            Err(DebugError::new(format!("no sample for {path}")))
                        })?;
                        hit = hit && value.is_low();
                    }
                    Ok(hit)
                })();

            let hit = match sample {
                Ok(hit) => {
                    stats.samples = stats.samples.saturating_add(1);
                    hit
                }
                Err(error) => {
                    stats.read_errors = stats.read_errors.saturating_add(1);
                    stats.last_error = Some(error.to_string());
                    stats.current_run = 0;
                    continue;
                }
            };

            if hit {
                stats.hits = stats.hits.saturating_add(1);
                stats.current_run = stats.current_run.saturating_add(1);
                stats.longest_run = stats.longest_run.max(stats.current_run);
                stats
                    .first_hit_time_steps
                    .get_or_insert(self.simulation_time_steps);
                stats.last_hit_time_steps = Some(self.simulation_time_steps);
            } else {
                stats.current_run = 0;
            }
        }
    }

    fn sample_recordings(&mut self) {
        let snapshot = &self.snapshot;
        for recording in self
            .recordings
            .values_mut()
            .filter(|recording| recording.active)
        {
            let values: Result<Vec<_>, _> = recording
                .spec
                .signals
                .iter()
                .map(|path| {
                    snapshot
                        .get(path)
                        .cloned()
                        .unwrap_or_else(|| Err(DebugError::new(format!("no sample for {path}"))))
                })
                .collect();
            match values {
                Ok(values) => recording.append(self.simulation_time_steps, values),
                Err(error) => {
                    recording.read_errors = recording.read_errors.saturating_add(1);
                    recording.last_error = Some(error.to_string());
                }
            }
        }
    }

    fn start_recording(
        &mut self,
        name: String,
        signals: Vec<String>,
        capacity: usize,
    ) -> Result<DebugResponse, DebugError> {
        if name.trim().is_empty() {
            return Err(DebugError::new("recording name cannot be empty"));
        }
        if self.recordings.contains_key(&name) {
            return Err(DebugError::new(format!(
                "recording '{name}' already exists"
            )));
        }
        if self.recordings.len() >= self.config.max_recordings {
            return Err(DebugError::new("maximum recording count reached"));
        }
        if signals.is_empty() || signals.len() > self.config.max_signals_per_recording {
            return Err(DebugError::new(format!(
                "recording must select between 1 and {} signals",
                self.config.max_signals_per_recording
            )));
        }
        if signals.iter().any(|path| path.trim().is_empty()) {
            return Err(DebugError::new("recording signal path cannot be empty"));
        }
        if signals.iter().collect::<BTreeSet<_>>().len() != signals.len() {
            return Err(DebugError::new("recording signal paths must be unique"));
        }
        if capacity == 0 || capacity > self.config.max_recording_capacity {
            return Err(DebugError::new(format!(
                "recording capacity must be between 1 and {}",
                self.config.max_recording_capacity
            )));
        }
        let values: Result<Vec<_>, _> = signals.iter().map(|path| self.read_cached(path)).collect();
        let mut recording = Recording {
            spec: RecordingSpec {
                name: name.clone(),
                signals,
                capacity,
            },
            active: true,
            samples: VecDeque::with_capacity(capacity),
            next_cursor: 0,
            dropped: 0,
            read_errors: 0,
            last_error: None,
        };
        recording.append(self.simulation_time_steps, values?);
        let status = recording.status();
        self.recordings.insert(name, recording);
        Ok(DebugResponse::RecordingStatus(status))
    }

    fn get_recording(
        &self,
        name: &str,
        cursor: Option<u64>,
        limit: usize,
    ) -> Result<DebugResponse, DebugError> {
        let recording = self
            .recordings
            .get(name)
            .ok_or_else(|| DebugError::new(format!("recording '{name}' does not exist")))?;
        let limit = limit.clamp(1, self.config.max_recording_page_size);
        let oldest = recording
            .samples
            .front()
            .map(|sample| sample.cursor)
            .unwrap_or(recording.next_cursor);
        let requested = cursor.unwrap_or(oldest);
        let truncated = requested < oldest;
        let start = requested.max(oldest);
        let samples: Vec<_> = recording
            .samples
            .iter()
            .filter(|sample| sample.cursor >= start)
            .take(limit)
            .cloned()
            .collect();
        let next_cursor = samples.last().and_then(|sample| {
            let next = sample.cursor.saturating_add(1);
            (next < recording.next_cursor).then_some(next)
        });
        Ok(DebugResponse::Recording(RecordingPage {
            name: name.to_owned(),
            samples,
            next_cursor,
            dropped: recording.dropped,
            truncated,
        }))
    }

    fn evaluate_predicate(&self, predicate: &PredicateSpec) -> Result<bool, DebugError> {
        let mut hit = true;
        for path in &predicate.all_high {
            let value = self
                .snapshot
                .get(path)
                .cloned()
                .unwrap_or_else(|| Err(DebugError::new(format!("no sample for {path}"))))?;
            hit = hit && value.is_high();
        }
        for path in &predicate.all_low {
            let value = self
                .snapshot
                .get(path)
                .cloned()
                .unwrap_or_else(|| Err(DebugError::new(format!("no sample for {path}"))))?;
            hit = hit && value.is_low();
        }
        Ok(hit)
    }

    fn arm_run_until_predicate(
        &mut self,
        predicate: PredicateSpec,
        timeout_steps: u64,
    ) -> Result<DebugResponse, DebugError> {
        predicate.validate()?;
        if timeout_steps == 0 {
            return Err(DebugError::new(
                "predicate timeout must be greater than zero",
            ));
        }
        let signal_count = predicate.all_high.len() + predicate.all_low.len();
        if signal_count > self.config.max_signals_per_recording {
            return Err(DebugError::new(format!(
                "predicate may contain at most {} signals",
                self.config.max_signals_per_recording
            )));
        }
        let timeout_time_steps = self
            .simulation_time_steps
            .checked_add(timeout_steps)
            .ok_or_else(|| DebugError::new("predicate timeout exceeds simulation time range"))?;
        for path in predicate.all_high.iter().chain(&predicate.all_low) {
            self.read_cached(path)?;
        }
        self.control = ControlStatus {
            mode: ControlMode::RunUntilPredicate,
            simulation_time_steps: self.simulation_time_steps,
            target_time_steps: None,
            predicate: Some(predicate),
            timeout_time_steps: Some(timeout_time_steps),
            pause_reason: None,
            last_error: None,
        };
        self.update_control_after_sample();
        Ok(DebugResponse::Control(self.control_status()))
    }

    fn update_control_after_sample(&mut self) {
        match self.control.mode {
            ControlMode::RunUntilTime => {
                if let Some(target) = self.control.target_time_steps {
                    if self.simulation_time_steps == target {
                        self.pause(PauseReason::TargetReached);
                    } else if self.simulation_time_steps > target {
                        self.control.last_error = Some(format!(
                            "run-until target {target} was missed at simulation time {}",
                            self.simulation_time_steps
                        ));
                        self.pause(PauseReason::DeadlineMissed);
                    }
                }
            }
            ControlMode::RunUntilPredicate => {
                if self
                    .control
                    .timeout_time_steps
                    .is_some_and(|timeout| self.simulation_time_steps > timeout)
                {
                    let timeout = self.control.timeout_time_steps.unwrap();
                    self.control.last_error = Some(format!(
                        "predicate timeout {timeout} was missed at simulation time {}",
                        self.simulation_time_steps
                    ));
                    self.pause(PauseReason::DeadlineMissed);
                    return;
                }
                let matched = self
                    .control
                    .predicate
                    .as_ref()
                    .map(|predicate| self.evaluate_predicate(predicate))
                    .transpose();
                match matched {
                    Ok(Some(true)) => self.pause(PauseReason::PredicateMatched),
                    Ok(Some(false)) => {
                        if self
                            .control
                            .timeout_time_steps
                            .is_some_and(|timeout| self.simulation_time_steps >= timeout)
                        {
                            self.pause(PauseReason::PredicateTimeout);
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        self.control.last_error = Some(error.to_string());
                        self.pause(PauseReason::PredicateReadError);
                    }
                }
            }
            ControlMode::Running | ControlMode::Paused | ControlMode::Terminated => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        cell::{Cell, RefCell},
        collections::HashMap,
        rc::Rc,
        thread,
    };

    struct MockProvider {
        values: HashMap<String, SignalValue>,
    }

    struct FailingProvider {
        reads: usize,
    }

    struct LateFailureProvider {
        reads: usize,
    }

    struct SharedProvider {
        values: Rc<RefCell<HashMap<String, SignalValue>>>,
    }

    struct CountingProvider {
        values: HashMap<String, SignalValue>,
        reads: Rc<Cell<usize>>,
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
            Ok(self
                .values
                .values()
                .map(|value| SignalInfo {
                    path: value.path.clone(),
                    kind: SignalKind::Logic,
                    width: Some(value.width),
                })
                .collect())
        }

        fn read_signal(&mut self, path: &str) -> Result<SignalValue, DebugError> {
            self.values
                .get(path)
                .cloned()
                .ok_or_else(|| DebugError::new(format!("unknown signal {path}")))
        }
    }

    impl SignalProvider for FailingProvider {
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
            self.reads += 1;
            if self.reads == 1 {
                Ok(bit(path, false))
            } else {
                Err(DebugError::new(format!("cannot read signal {path}")))
            }
        }
    }

    impl SignalProvider for LateFailureProvider {
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
            self.reads += 1;
            match self.reads {
                1 | 2 => Ok(bit(path, true)),
                3 => Ok(bit(path, false)),
                _ => Err(DebugError::new(format!("cannot read signal {path}"))),
            }
        }
    }

    impl SignalProvider for SharedProvider {
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
                .borrow()
                .get(path)
                .cloned()
                .ok_or_else(|| DebugError::new(format!("unknown signal {path}")))
        }
    }

    impl SignalProvider for CountingProvider {
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
            self.reads.set(self.reads.get() + 1);
            self.values
                .get(path)
                .cloned()
                .ok_or_else(|| DebugError::new(format!("unknown signal {path}")))
        }
    }

    fn bit(path: &str, value: bool) -> SignalValue {
        SignalValue {
            path: path.into(),
            width: 1,
            binary: if value { "1" } else { "0" }.into(),
        }
    }

    #[test]
    fn recording_starts_with_snapshot_then_appends_only_changes() {
        let values = Rc::new(RefCell::new(HashMap::from([(
            "dut.count".into(),
            SignalValue {
                path: "dut.count".into(),
                width: 2,
                binary: "00".into(),
            },
        )])));
        let (mut session, _client) = DebugSession::new(SharedProvider {
            values: values.clone(),
        });
        session.poll(10);

        let DebugResponse::RecordingStatus(status) = session
            .handle(DebugRequest::StartRecording {
                name: "count".into(),
                signals: vec!["dut.count".into()],
                capacity: 4,
            })
            .unwrap()
        else {
            panic!("wrong response");
        };
        assert_eq!(status.retained, 1);

        session.poll(10);
        session.poll(11);
        values.borrow_mut().insert(
            "dut.count".into(),
            SignalValue {
                path: "dut.count".into(),
                width: 2,
                binary: "01".into(),
            },
        );
        session.poll(12);

        let DebugResponse::Recording(page) = session
            .handle(DebugRequest::GetRecording {
                name: "count".into(),
                cursor: None,
                limit: 10,
            })
            .unwrap()
        else {
            panic!("wrong response");
        };
        assert_eq!(page.samples.len(), 2);
        assert_eq!(page.samples[0].cursor, 0);
        assert_eq!(page.samples[0].simulation_time_steps, 10);
        assert_eq!(page.samples[0].values[0].binary, "00");
        assert_eq!(page.samples[1].cursor, 1);
        assert_eq!(page.samples[1].simulation_time_steps, 12);
        assert_eq!(page.samples[1].values[0].binary, "01");
    }

    #[test]
    fn recording_ring_reports_overflow_and_paginates_by_cursor() {
        let values = Rc::new(RefCell::new(HashMap::from([(
            "dut.count".into(),
            SignalValue {
                path: "dut.count".into(),
                width: 2,
                binary: "00".into(),
            },
        )])));
        let (mut session, _client) = DebugSession::new(SharedProvider {
            values: values.clone(),
        });
        session.poll(0);
        session
            .handle(DebugRequest::StartRecording {
                name: "tiny".into(),
                signals: vec!["dut.count".into()],
                capacity: 2,
            })
            .unwrap();
        for (time, binary) in [(1, "01"), (2, "10")] {
            values.borrow_mut().get_mut("dut.count").unwrap().binary = binary.into();
            session.poll(time);
        }

        let DebugResponse::Recording(first) = session
            .handle(DebugRequest::GetRecording {
                name: "tiny".into(),
                cursor: Some(0),
                limit: 1,
            })
            .unwrap()
        else {
            panic!("wrong response");
        };
        assert!(first.truncated);
        assert_eq!(first.dropped, 1);
        assert_eq!(first.samples[0].cursor, 1);
        assert_eq!(first.next_cursor, Some(2));

        let DebugResponse::Recording(second) = session
            .handle(DebugRequest::GetRecording {
                name: "tiny".into(),
                cursor: first.next_cursor,
                limit: 1,
            })
            .unwrap()
        else {
            panic!("wrong response");
        };
        assert!(!second.truncated);
        assert_eq!(second.samples[0].cursor, 2);
        assert_eq!(second.next_cursor, None);
    }

    #[test]
    fn recordings_stop_remove_and_sample_independently() {
        let values = Rc::new(RefCell::new(HashMap::from([
            ("dut.a".into(), bit("dut.a", false)),
            ("dut.b".into(), bit("dut.b", false)),
        ])));
        let (mut session, _client) = DebugSession::new(SharedProvider {
            values: values.clone(),
        });
        session.poll(0);
        for (name, path) in [("a", "dut.a"), ("b", "dut.b")] {
            session
                .handle(DebugRequest::StartRecording {
                    name: name.into(),
                    signals: vec![path.into()],
                    capacity: 4,
                })
                .unwrap();
        }
        session
            .handle(DebugRequest::StopRecording { name: "a".into() })
            .unwrap();
        values.borrow_mut().get_mut("dut.a").unwrap().binary = "1".into();
        values.borrow_mut().get_mut("dut.b").unwrap().binary = "1".into();
        session.poll(1);

        let DebugResponse::RecordingStatus(a) = session
            .handle(DebugRequest::RecordingStatus { name: "a".into() })
            .unwrap()
        else {
            panic!("wrong response");
        };
        let DebugResponse::RecordingStatus(b) = session
            .handle(DebugRequest::RecordingStatus { name: "b".into() })
            .unwrap()
        else {
            panic!("wrong response");
        };
        assert!(!a.active);
        assert_eq!(a.retained, 1);
        assert!(b.active);
        assert_eq!(b.retained, 2);
        assert_eq!(
            session
                .handle(DebugRequest::RemoveRecording { name: "a".into() })
                .unwrap(),
            DebugResponse::Removed(true)
        );
        assert!(session
            .handle(DebugRequest::RecordingStatus { name: "a".into() })
            .is_err());
    }

    #[test]
    fn recording_and_response_limits_are_enforced() {
        let provider = MockProvider {
            values: HashMap::from([
                ("dut.a".into(), bit("dut.a", false)),
                ("dut.b".into(), bit("dut.b", true)),
            ]),
        };
        let (mut session, _client) = DebugSession::with_config(
            provider,
            DebugSessionConfig {
                max_recordings: 1,
                max_signals_per_recording: 1,
                max_recording_capacity: 2,
                max_recording_page_size: 1,
                max_hierarchy_results: 1,
                ..DebugSessionConfig::default()
            },
        )
        .unwrap();
        session.poll(0);

        assert_eq!(
            session
                .handle(DebugRequest::StartRecording {
                    name: "too_wide".into(),
                    signals: vec!["dut.a".into(), "dut.b".into()],
                    capacity: 1,
                })
                .unwrap_err()
                .message,
            "recording must select between 1 and 1 signals"
        );
        assert_eq!(
            session
                .handle(DebugRequest::StartRecording {
                    name: "too_deep".into(),
                    signals: vec!["dut.a".into()],
                    capacity: 3,
                })
                .unwrap_err()
                .message,
            "recording capacity must be between 1 and 2"
        );
        session
            .handle(DebugRequest::StartRecording {
                name: "a".into(),
                signals: vec!["dut.a".into()],
                capacity: 2,
            })
            .unwrap();
        assert_eq!(
            session
                .handle(DebugRequest::StartRecording {
                    name: "b".into(),
                    signals: vec!["dut.b".into()],
                    capacity: 2,
                })
                .unwrap_err()
                .message,
            "maximum recording count reached"
        );

        session
            .recordings
            .get_mut("a")
            .unwrap()
            .append(1, vec![bit("dut.a", true)]);
        let DebugResponse::Recording(page) = session
            .handle(DebugRequest::GetRecording {
                name: "a".into(),
                cursor: None,
                limit: usize::MAX,
            })
            .unwrap()
        else {
            panic!("wrong response");
        };
        assert_eq!(page.samples.len(), 1);
        assert_eq!(page.next_cursor, Some(1));

        let DebugResponse::Hierarchy(entries) = session
            .handle(DebugRequest::ListHierarchy {
                path: String::new(),
                max_depth: usize::MAX,
                max_results: usize::MAX,
            })
            .unwrap()
        else {
            panic!("wrong response");
        };
        assert_eq!(entries.len(), 1);
    }

    #[test]
    fn watch_count_and_signal_limits_are_enforced() {
        let provider = MockProvider {
            values: HashMap::from([
                ("dut.a".into(), bit("dut.a", false)),
                ("dut.b".into(), bit("dut.b", true)),
            ]),
        };
        let (mut session, _client) = DebugSession::with_config(
            provider,
            DebugSessionConfig {
                max_watches: 1,
                max_signals_per_watch: 1,
                ..DebugSessionConfig::default()
            },
        )
        .unwrap();
        session.poll(0);

        assert_eq!(
            session
                .handle(DebugRequest::AddWatch {
                    spec: WatchSpec {
                        name: "too_wide".into(),
                        all_high: vec!["dut.a".into()],
                        all_low: vec!["dut.b".into()],
                    },
                })
                .unwrap_err()
                .message,
            "watch must select between 1 and 1 signals"
        );
        session
            .handle(DebugRequest::AddWatch {
                spec: WatchSpec {
                    name: "a".into(),
                    all_high: vec!["dut.a".into()],
                    all_low: Vec::new(),
                },
            })
            .unwrap();
        assert_eq!(
            session
                .handle(DebugRequest::AddWatch {
                    spec: WatchSpec {
                        name: "b".into(),
                        all_high: vec!["dut.b".into()],
                        all_low: Vec::new(),
                    },
                })
                .unwrap_err()
                .message,
            "maximum watch count reached"
        );

        let DebugResponse::Watch(replaced) = session
            .handle(DebugRequest::AddWatch {
                spec: WatchSpec {
                    name: "a".into(),
                    all_high: vec!["dut.b".into()],
                    all_low: Vec::new(),
                },
            })
            .unwrap()
        else {
            panic!("wrong response");
        };
        assert_eq!(replaced.spec.all_high, vec!["dut.b"]);
        assert_eq!(session.watches.len(), 1);
    }

    #[test]
    fn zero_session_limits_are_rejected() {
        for config in [
            DebugSessionConfig {
                request_queue_capacity: 0,
                ..DebugSessionConfig::default()
            },
            DebugSessionConfig {
                max_requests_per_poll: 0,
                ..DebugSessionConfig::default()
            },
            DebugSessionConfig {
                max_watches: 0,
                ..DebugSessionConfig::default()
            },
            DebugSessionConfig {
                max_signals_per_watch: 0,
                ..DebugSessionConfig::default()
            },
            DebugSessionConfig {
                max_recordings: 0,
                ..DebugSessionConfig::default()
            },
            DebugSessionConfig {
                max_signals_per_recording: 0,
                ..DebugSessionConfig::default()
            },
            DebugSessionConfig {
                max_recording_capacity: 0,
                ..DebugSessionConfig::default()
            },
            DebugSessionConfig {
                max_recording_page_size: 0,
                ..DebugSessionConfig::default()
            },
            DebugSessionConfig {
                max_hierarchy_results: 0,
                ..DebugSessionConfig::default()
            },
            DebugSessionConfig {
                pause_inactivity_timeout: Duration::ZERO,
                ..DebugSessionConfig::default()
            },
        ] {
            assert!(DebugSession::with_config(
                MockProvider {
                    values: HashMap::new()
                },
                config
            )
            .is_err());
        }
    }

    #[test]
    fn recording_read_failures_are_reported_without_false_changes() {
        let (mut session, _client) = DebugSession::new(FailingProvider { reads: 0 });
        session.poll(0);
        session
            .handle(DebugRequest::StartRecording {
                name: "unreadable".into(),
                signals: vec!["dut.value".into()],
                capacity: 4,
            })
            .unwrap();
        session.poll(1);

        let DebugResponse::RecordingStatus(status) = session
            .handle(DebugRequest::RecordingStatus {
                name: "unreadable".into(),
            })
            .unwrap()
        else {
            panic!("wrong response");
        };
        assert_eq!(status.retained, 1);
        assert_eq!(status.read_errors, 1);
        assert_eq!(
            status.last_error.as_deref(),
            Some("cannot read signal dut.value")
        );
    }

    #[test]
    fn same_time_polls_do_not_resample_and_consumers_share_reads() {
        let reads = Rc::new(Cell::new(0));
        let provider = CountingProvider {
            values: HashMap::from([("dut.ready".into(), bit("dut.ready", true))]),
            reads: reads.clone(),
        };
        let (mut session, _client) = DebugSession::new(provider);
        session.poll(0);
        assert_eq!(reads.get(), 0, "normal mode must not read provider signals");
        session
            .handle(DebugRequest::StartRecording {
                name: "ready".into(),
                signals: vec!["dut.ready".into()],
                capacity: 4,
            })
            .unwrap();
        session
            .handle(DebugRequest::AddWatch {
                spec: WatchSpec {
                    name: "ready".into(),
                    all_high: vec!["dut.ready".into()],
                    all_low: Vec::new(),
                },
            })
            .unwrap();
        session
            .handle(DebugRequest::RunUntilPredicate {
                predicate: PredicateSpec {
                    all_high: Vec::new(),
                    all_low: vec!["dut.ready".into()],
                },
                timeout_steps: 4,
            })
            .unwrap();
        assert_eq!(reads.get(), 1);

        session.poll(0);
        session.poll(0);
        assert_eq!(reads.get(), 1);
        session.poll(1);
        assert_eq!(reads.get(), 2);
        assert_eq!(
            session.control_status().mode,
            ControlMode::RunUntilPredicate
        );

        let DebugResponse::Watches(watches) = session.handle(DebugRequest::ListWatches).unwrap()
        else {
            panic!("wrong response");
        };
        assert_eq!(watches[0].samples, 1);
        let DebugResponse::RecordingStatus(status) = session
            .handle(DebugRequest::RecordingStatus {
                name: "ready".into(),
            })
            .unwrap()
        else {
            panic!("wrong response");
        };
        assert_eq!(status.retained, 1);
    }

    #[test]
    fn control_pause_resume_and_run_until_time_arm_without_overshoot() {
        let provider = MockProvider {
            values: HashMap::new(),
        };
        let (mut session, _client) = DebugSession::new(provider);
        session.poll(10);

        let DebugResponse::Control(paused) = session.handle(DebugRequest::Pause).unwrap() else {
            panic!("wrong response");
        };
        assert_eq!(paused.mode, ControlMode::Paused);
        assert_eq!(paused.pause_reason, Some(PauseReason::Requested));

        let DebugResponse::Control(running) = session.handle(DebugRequest::Resume).unwrap() else {
            panic!("wrong response");
        };
        assert_eq!(running.mode, ControlMode::Running);

        let DebugResponse::Control(armed) = session
            .handle(DebugRequest::RunUntilTime {
                target_time_steps: 12,
            })
            .unwrap()
        else {
            panic!("wrong response");
        };
        assert_eq!(armed.mode, ControlMode::RunUntilTime);
        assert_eq!(armed.target_time_steps, Some(12));

        session.poll(11);
        assert_eq!(session.control_status().mode, ControlMode::RunUntilTime);
        session.poll(12);
        assert_eq!(session.control_status().mode, ControlMode::Paused);
        assert_eq!(
            session.control_status().pause_reason,
            Some(PauseReason::TargetReached)
        );
        assert!(session
            .handle(DebugRequest::RunUntilTime {
                target_time_steps: 11,
            })
            .is_err());
    }

    #[test]
    fn missed_exact_deadlines_pause_with_an_observable_error() {
        let provider = MockProvider {
            values: HashMap::from([("dut.done".into(), bit("dut.done", false))]),
        };
        let (mut session, _client) = DebugSession::new(provider);
        session.poll(10);
        session
            .handle(DebugRequest::RunUntilTime {
                target_time_steps: 12,
            })
            .unwrap();
        session.poll(13);
        assert_eq!(
            session.control_status().pause_reason,
            Some(PauseReason::DeadlineMissed)
        );
        assert_eq!(
            session.control_status().last_error.as_deref(),
            Some("run-until target 12 was missed at simulation time 13")
        );

        session.handle(DebugRequest::Resume).unwrap();
        session
            .handle(DebugRequest::RunUntilPredicate {
                predicate: PredicateSpec {
                    all_high: vec!["dut.done".into()],
                    all_low: Vec::new(),
                },
                timeout_steps: 2,
            })
            .unwrap();
        session.poll(16);
        assert_eq!(
            session.control_status().pause_reason,
            Some(PauseReason::DeadlineMissed)
        );
        assert_eq!(
            session.control_status().last_error.as_deref(),
            Some("predicate timeout 15 was missed at simulation time 16")
        );
    }

    #[test]
    fn run_until_predicate_pauses_on_match_or_exact_timeout_and_terminates() {
        let values = Rc::new(RefCell::new(HashMap::from([(
            "dut.done".into(),
            bit("dut.done", false),
        )])));
        let (mut session, _client) = DebugSession::new(SharedProvider {
            values: values.clone(),
        });
        session.poll(0);
        let predicate = PredicateSpec {
            all_high: vec!["dut.done".into()],
            all_low: Vec::new(),
        };
        let DebugResponse::Control(armed) = session
            .handle(DebugRequest::RunUntilPredicate {
                predicate: predicate.clone(),
                timeout_steps: 5,
            })
            .unwrap()
        else {
            panic!("wrong response");
        };
        assert_eq!(armed.mode, ControlMode::RunUntilPredicate);
        assert_eq!(armed.timeout_time_steps, Some(5));
        session.poll(1);
        values.borrow_mut().get_mut("dut.done").unwrap().binary = "1".into();
        session.poll(2);
        assert_eq!(
            session.control_status().pause_reason,
            Some(PauseReason::PredicateMatched)
        );

        session.handle(DebugRequest::Resume).unwrap();
        values.borrow_mut().get_mut("dut.done").unwrap().binary = "0".into();
        session.poll(3);
        session
            .handle(DebugRequest::RunUntilPredicate {
                predicate,
                timeout_steps: 2,
            })
            .unwrap();
        session.poll(4);
        assert_eq!(
            session.control_status().mode,
            ControlMode::RunUntilPredicate
        );
        session.poll(5);
        assert_eq!(session.control_status().mode, ControlMode::Paused);
        assert_eq!(
            session.control_status().pause_reason,
            Some(PauseReason::PredicateTimeout)
        );

        let DebugResponse::Control(terminated) = session.handle(DebugRequest::Terminate).unwrap()
        else {
            panic!("wrong response");
        };
        assert_eq!(terminated.mode, ControlMode::Terminated);
    }

    #[test]
    fn paused_session_services_live_reads_and_resume_at_frozen_time() {
        let provider = MockProvider {
            values: HashMap::from([("dut.ready".into(), bit("dut.ready", true))]),
        };
        let (mut session, client) = DebugSession::new(provider);
        session.poll(9);
        session.handle(DebugRequest::Pause).unwrap();

        let read_client = client.clone();
        let read = thread::spawn(move || {
            read_client.request(DebugRequest::ReadSignal {
                path: "dut.ready".into(),
            })
        });
        assert_eq!(
            session.poll_wait(9, Duration::from_secs(1)),
            DebugDirective::Hold
        );
        assert_eq!(
            read.join().unwrap().unwrap(),
            DebugResponse::Signal(bit("dut.ready", true))
        );
        assert_eq!(session.control_status().simulation_time_steps, 9);

        let resume = thread::spawn(move || client.request(DebugRequest::Resume));
        assert_eq!(
            session.poll_wait(9, Duration::from_secs(1)),
            DebugDirective::Advance
        );
        resume.join().unwrap().unwrap();
        assert_eq!(session.control_status().simulation_time_steps, 9);
    }

    #[test]
    fn controller_disconnect_fails_open_from_pause() {
        let provider = MockProvider {
            values: HashMap::new(),
        };
        let (mut session, client) = DebugSession::new(provider);
        session.poll(4);
        session.handle(DebugRequest::Pause).unwrap();
        drop(client);

        assert_eq!(
            session.poll_wait(4, Duration::from_millis(1)),
            DebugDirective::Advance
        );
        assert!(!session.controller_connected());
        assert_eq!(session.control_status().mode, ControlMode::Running);
        assert_eq!(
            session.control_status().pause_reason,
            Some(PauseReason::ControllerDisconnected)
        );
    }

    #[test]
    fn inactive_pause_lease_fails_open_without_advancing_time() {
        let provider = MockProvider {
            values: HashMap::new(),
        };
        let (mut session, _client) = DebugSession::with_config(
            provider,
            DebugSessionConfig {
                pause_inactivity_timeout: Duration::from_millis(5),
                ..DebugSessionConfig::default()
            },
        )
        .unwrap();
        session.poll(4);
        session.handle(DebugRequest::Pause).unwrap();

        assert_eq!(
            session.poll_wait(4, Duration::from_millis(20)),
            DebugDirective::Advance
        );
        assert_eq!(session.control_status().simulation_time_steps, 4);
        assert_eq!(
            session.control_status().pause_reason,
            Some(PauseReason::LeaseExpired)
        );
    }

    #[test]
    fn reaching_a_run_until_target_starts_a_fresh_pause_lease() {
        let provider = MockProvider {
            values: HashMap::new(),
        };
        let (mut session, _client) = DebugSession::with_config(
            provider,
            DebugSessionConfig {
                pause_inactivity_timeout: Duration::from_millis(20),
                ..DebugSessionConfig::default()
            },
        )
        .unwrap();
        session.poll(0);
        session
            .handle(DebugRequest::RunUntilTime {
                target_time_steps: 2,
            })
            .unwrap();
        session.last_request_activity = Instant::now() - Duration::from_secs(1);

        assert_eq!(session.poll(2), DebugDirective::Hold);
        assert_eq!(
            session.poll_wait(2, Duration::from_millis(1)),
            DebugDirective::Hold
        );
        assert_eq!(
            session.control_status().pause_reason,
            Some(PauseReason::TargetReached)
        );
    }

    #[test]
    fn termination_is_sticky_against_later_control_requests() {
        let provider = MockProvider {
            values: HashMap::new(),
        };
        let (mut session, _client) = DebugSession::new(provider);
        session.poll(7);
        session.handle(DebugRequest::Terminate).unwrap();

        assert_eq!(
            session.handle(DebugRequest::Resume).unwrap_err().message,
            "debug session is terminated"
        );
        assert_eq!(session.control_status().mode, ControlMode::Terminated);
        assert_eq!(session.directive(), DebugDirective::Terminate);
    }

    #[test]
    fn run_until_predicate_read_error_is_not_reported_as_timeout() {
        let (mut session, _client) = DebugSession::new(FailingProvider { reads: 0 });
        session.poll(0);
        session
            .handle(DebugRequest::RunUntilPredicate {
                predicate: PredicateSpec {
                    all_high: vec!["dut.done".into()],
                    all_low: Vec::new(),
                },
                timeout_steps: 10,
            })
            .unwrap();
        session.poll(1);

        let status = session.control_status();
        assert_eq!(status.mode, ControlMode::Paused);
        assert_eq!(status.pause_reason, Some(PauseReason::PredicateReadError));
        assert_eq!(
            status.last_error.as_deref(),
            Some("cannot read signal dut.done")
        );
    }

    #[test]
    fn client_requests_are_executed_only_when_simulator_polls() {
        let provider = MockProvider {
            values: HashMap::from([("dut.ready".into(), bit("dut.ready", true))]),
        };
        let (mut session, client) = DebugSession::new(provider);
        let request = thread::spawn(move || {
            client.request(DebugRequest::ReadSignal {
                path: "dut.ready".into(),
            })
        });
        thread::sleep(Duration::from_millis(10));
        session.poll(17);
        assert_eq!(
            request.join().unwrap().unwrap(),
            DebugResponse::Signal(bit("dut.ready", true))
        );
    }

    #[test]
    fn watch_counts_valid_without_ready_runs() {
        let provider = MockProvider {
            values: HashMap::from([
                ("dut.valid".into(), bit("dut.valid", true)),
                ("dut.ready".into(), bit("dut.ready", false)),
            ]),
        };
        let (mut session, _client) = DebugSession::new(provider);
        session
            .handle(DebugRequest::AddWatch {
                spec: WatchSpec {
                    name: "stalled".into(),
                    all_high: vec!["dut.valid".into()],
                    all_low: vec!["dut.ready".into()],
                },
            })
            .unwrap();
        session.poll(20);
        session.poll(21);
        let DebugResponse::Watches(watches) = session.handle(DebugRequest::ListWatches).unwrap()
        else {
            panic!("wrong response");
        };
        assert_eq!(watches[0].hits, 2);
        assert_eq!(watches[0].longest_run, 2);
        assert_eq!(watches[0].first_hit_time_steps, Some(20));
        assert_eq!(watches[0].last_hit_time_steps, Some(21));
    }

    #[test]
    fn timed_out_add_watch_is_not_applied_by_a_later_poll() {
        let provider = MockProvider {
            values: HashMap::from([("dut.valid".into(), bit("dut.valid", true))]),
        };
        let (mut session, client) = DebugSession::new(provider);

        let error = client
            .request_timeout(
                DebugRequest::AddWatch {
                    spec: WatchSpec {
                        name: "late".into(),
                        all_high: vec!["dut.valid".into()],
                        all_low: Vec::new(),
                    },
                },
                Duration::from_millis(1),
            )
            .unwrap_err();
        assert_eq!(
            error.message,
            "debug request timed out waiting for simulator poll"
        );

        session.poll(1);
        let DebugResponse::Watches(watches) = session.handle(DebugRequest::ListWatches).unwrap()
        else {
            panic!("wrong response");
        };
        assert!(watches.is_empty());
    }

    #[test]
    fn a_claimed_request_returns_its_result_instead_of_a_false_timeout() {
        let provider = MockProvider {
            values: HashMap::new(),
        };
        let (mut session, client) = DebugSession::new(provider);
        let request = thread::spawn(move || {
            client.request_timeout(DebugRequest::Pause, Duration::from_millis(1))
        });
        let envelope = session
            .requests
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        envelope
            .state
            .compare_exchange(
                REQUEST_PENDING,
                REQUEST_CLAIMED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .unwrap();
        thread::sleep(Duration::from_millis(5));
        let result = session.handle(envelope.request);
        envelope.response.send(result).unwrap();

        let DebugResponse::Control(status) = request.join().unwrap().unwrap() else {
            panic!("wrong response");
        };
        assert_eq!(status.mode, ControlMode::Paused);
    }

    #[test]
    fn add_watch_rejects_unknown_signals() {
        let provider = MockProvider {
            values: HashMap::new(),
        };
        let (mut session, _client) = DebugSession::new(provider);

        let error = session
            .handle(DebugRequest::AddWatch {
                spec: WatchSpec {
                    name: "invalid".into(),
                    all_high: vec!["dut.missing".into()],
                    all_low: Vec::new(),
                },
            })
            .unwrap_err();

        assert_eq!(error.message, "unknown signal dut.missing");
        assert!(session.watches.is_empty());
    }

    #[test]
    fn watch_read_failures_are_reported_without_counting_a_sample() {
        let (mut session, _client) = DebugSession::new(FailingProvider { reads: 0 });
        session
            .handle(DebugRequest::AddWatch {
                spec: WatchSpec {
                    name: "unreadable".into(),
                    all_high: vec!["dut.valid".into()],
                    all_low: Vec::new(),
                },
            })
            .unwrap();

        session.poll(12);
        let DebugResponse::Watches(watches) = session.handle(DebugRequest::ListWatches).unwrap()
        else {
            panic!("wrong response");
        };
        assert_eq!(watches[0].samples, 0);
        assert_eq!(watches[0].hits, 0);
        assert_eq!(watches[0].read_errors, 1);
        assert_eq!(
            watches[0].last_error.as_deref(),
            Some("cannot read signal dut.valid")
        );
    }

    #[test]
    fn false_predicate_does_not_hide_a_later_signal_read_failure() {
        let (mut session, client) = DebugSession::new(LateFailureProvider { reads: 0 });
        let add_client = client.clone();
        let add_watch = thread::spawn(move || {
            add_client.request(DebugRequest::AddWatch {
                spec: WatchSpec {
                    name: "late_failure".into(),
                    all_high: vec!["dut.first".into(), "dut.second".into()],
                    all_low: Vec::new(),
                },
            })
        });
        thread::sleep(Duration::from_millis(10));
        session.poll(0);
        add_watch.join().unwrap().unwrap();

        let list_watches = thread::spawn(move || client.request(DebugRequest::ListWatches));
        thread::sleep(Duration::from_millis(10));
        session.poll(1);
        let DebugResponse::Watches(watches) = list_watches.join().unwrap().unwrap() else {
            panic!("wrong response");
        };

        assert_eq!(watches[0].samples, 0);
        assert_eq!(watches[0].hits, 0);
        assert_eq!(watches[0].read_errors, 1);
        assert_eq!(
            watches[0].last_error.as_deref(),
            Some("cannot read signal dut.second")
        );
    }

    #[test]
    fn request_queue_rejects_work_beyond_its_capacity() {
        let provider = MockProvider {
            values: HashMap::new(),
        };
        let (_session, client) = DebugSession::with_config(
            provider,
            DebugSessionConfig {
                request_queue_capacity: 1,
                max_requests_per_poll: 1,
                ..DebugSessionConfig::default()
            },
        )
        .unwrap();

        client
            .request_timeout(DebugRequest::Status, Duration::from_millis(1))
            .unwrap_err();
        let error = client
            .request_timeout(DebugRequest::Status, Duration::from_secs(1))
            .unwrap_err();
        assert_eq!(error.message, "debug request queue is full");
    }

    #[test]
    fn simulator_poll_services_only_its_configured_request_budget() {
        let provider = MockProvider {
            values: HashMap::from([("dut.valid".into(), bit("dut.valid", true))]),
        };
        let (mut session, client) = DebugSession::with_config(
            provider,
            DebugSessionConfig {
                request_queue_capacity: 3,
                max_requests_per_poll: 1,
                ..DebugSessionConfig::default()
            },
        )
        .unwrap();

        for name in ["first", "second", "third"] {
            let (response, _ignored) = mpsc::channel();
            client
                .requests
                .try_send(RequestEnvelope {
                    request: DebugRequest::AddWatch {
                        spec: WatchSpec {
                            name: name.into(),
                            all_high: vec!["dut.valid".into()],
                            all_low: Vec::new(),
                        },
                    },
                    response,
                    deadline: Instant::now() + Duration::from_secs(1),
                    state: Arc::new(AtomicU8::new(REQUEST_PENDING)),
                })
                .unwrap();
        }

        session.poll(1);
        assert_eq!(session.watches.len(), 1);
        session.poll(2);
        assert_eq!(session.watches.len(), 2);
        session.poll(3);
        assert_eq!(session.watches.len(), 3);
    }
}
