//! Private, runtime-gated FST recording for the Verilator MCP adapter.
//!
//! Nothing in this module is part of `rustdv-debug`: capture capability,
//! temporary files and FST decoding are properties of this adapter and its
//! simulator host.  Live reads, watches and run predicates continue to use the
//! backend-neutral debug session.

use fst_reader::{
    FstFilter, FstHierarchyEntry, FstReader, FstSignalHandle, FstSignalValue, ReadSignalsError,
};
use rustdv::sim::verilator_trace;
use rustdv_debug::{
    DebugError, RecordingPage, RecordingSample, RecordingSpec, SignalInfo, SignalKind, SignalValue,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    fs::File,
    io::BufReader,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU8, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, Sender, SyncSender, TryRecvError, TrySendError},
        Arc,
    },
    time::{Duration, Instant},
};
use tempfile::TempDir;

const REQUEST_PENDING: u8 = 0;
const REQUEST_CLAIMED: u8 = 1;
const REQUEST_CANCELLED: u8 = 2;

/// Resource bounds for private FST capture and historical queries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceRecordingConfig {
    pub request_queue_capacity: usize,
    pub max_requests_per_poll: usize,
    pub max_recordings: usize,
    pub max_signals_per_recording: usize,
    pub max_recording_capacity: usize,
    pub max_recording_page_size: usize,
    pub max_hierarchy_results: usize,
    pub max_indexed_hierarchy_entries: usize,
    pub max_hierarchy_path_bytes: usize,
    pub max_snapshot_signals: usize,
    pub max_changes_per_query: usize,
    pub max_changes_scanned: usize,
    /// Total decoded-value budget for filtered history queries.
    pub max_decoded_bytes_per_query: usize,
    /// Maximum width, in encoded bytes, of one value retained by the
    /// streaming legacy projection. Projection work scans each newly closed
    /// segment once, but retains only the configured recording ring.
    pub max_projected_value_bytes: usize,
    pub max_segments_per_recording: usize,
    /// Stop threshold checked after flushing the active FST. The file can
    /// exceed this value by data emitted since the preceding check.
    pub max_bytes_per_recording: u64,
    pub max_duration_steps: u64,
    /// Simulation-time interval between an FST flush and disk-limit check.
    pub limit_check_interval_steps: u64,
}

impl Default for TraceRecordingConfig {
    fn default() -> Self {
        Self {
            request_queue_capacity: 64,
            max_requests_per_poll: 16,
            max_recordings: 8,
            max_signals_per_recording: 64,
            max_recording_capacity: 4096,
            max_recording_page_size: 256,
            max_hierarchy_results: 4096,
            max_indexed_hierarchy_entries: 1_000_000,
            max_hierarchy_path_bytes: 16 * 1024,
            max_snapshot_signals: 512,
            max_changes_per_query: 4096,
            max_changes_scanned: 1_000_000,
            max_decoded_bytes_per_query: 64 * 1024 * 1024,
            max_projected_value_bytes: 1024 * 1024,
            max_segments_per_recording: 64,
            max_bytes_per_recording: 512 * 1024 * 1024,
            max_duration_steps: 100_000_000,
            limit_check_interval_steps: 100_000,
        }
    }
}

impl TraceRecordingConfig {
    fn validate(self) -> Result<Self, DebugError> {
        if self.request_queue_capacity == 0
            || self.max_requests_per_poll == 0
            || self.max_recordings == 0
            || self.max_signals_per_recording == 0
            || self.max_recording_capacity == 0
            || self.max_recording_page_size == 0
            || self.max_hierarchy_results == 0
            || self.max_indexed_hierarchy_entries == 0
            || self.max_hierarchy_path_bytes == 0
            || self.max_snapshot_signals == 0
            || self.max_changes_per_query == 0
            || self.max_changes_scanned == 0
            || self.max_decoded_bytes_per_query == 0
            || self.max_projected_value_bytes == 0
            || self.max_segments_per_recording == 0
            || self.max_bytes_per_recording == 0
            || self.max_duration_steps == 0
            || self.limit_check_interval_steps == 0
        {
            return Err(DebugError::new(
                "FST recording and query limits must all be greater than zero",
            ));
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceRecordingStatus {
    // The first fields intentionally match rustdv-debug's RecordingStatus so
    // existing JSON clients can ignore the additional backend information.
    pub spec: RecordingSpec,
    pub active: bool,
    pub retained: usize,
    pub dropped: u64,
    pub next_cursor: u64,
    pub read_errors: u64,
    pub last_error: Option<String>,
    pub backend: String,
    pub start_time_steps: u64,
    pub end_time_steps: u64,
    pub bytes: u64,
    pub signal_count: usize,
    pub segments: usize,
    pub limit_reached: bool,
    pub limit_reason: Option<String>,
    /// Exact through this time when stopped; last materialized query time while active.
    pub projection_through_time_steps: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceSummary {
    pub backend: String,
    pub available: bool,
    pub recordings: usize,
    pub active_recording: Option<String>,
    pub retained_bytes: u64,
    pub max_bytes_per_recording: u64,
    pub max_duration_steps: u64,
    pub max_recordings: usize,
    pub max_signals_per_recording: usize,
    pub max_recording_capacity: usize,
    pub max_recording_page_size: usize,
    pub max_hierarchy_results: usize,
    pub max_indexed_hierarchy_entries: usize,
    pub max_hierarchy_path_bytes: usize,
    pub max_snapshot_signals: usize,
    pub max_changes_per_query: usize,
    pub max_changes_scanned: usize,
    pub max_decoded_bytes_per_query: usize,
    pub max_projected_value_bytes: usize,
    pub max_segments_per_recording: usize,
    pub limit_check_interval_steps: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceValueAt {
    pub name: String,
    pub requested_time_steps: u64,
    pub sampled_time_steps: u64,
    pub value: SignalValue,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceChange {
    pub cursor: u64,
    pub simulation_time_steps: u64,
    pub value: SignalValue,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceChangePage {
    pub name: String,
    pub signal: String,
    pub changes: Vec<TraceChange>,
    pub next_cursor: Option<u64>,
    pub truncated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceSnapshot {
    pub name: String,
    pub scope: String,
    pub requested_time_steps: u64,
    pub values: Vec<SignalValue>,
    pub truncated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "response", content = "value", rename_all = "snake_case")]
pub enum TraceResponse {
    RecordingStatus(TraceRecordingStatus),
    Recording(RecordingPage),
    Hierarchy(Vec<SignalInfo>),
    ValueAt(TraceValueAt),
    Changes(TraceChangePage),
    Snapshot(TraceSnapshot),
    Summary(TraceSummary),
    Removed(bool),
}

#[derive(Clone, Debug)]
pub(crate) enum TraceRequest {
    Start {
        name: String,
        signals: Vec<String>,
        capacity: usize,
    },
    Status {
        name: String,
    },
    Get {
        name: String,
        cursor: Option<u64>,
        limit: usize,
    },
    Stop {
        name: String,
    },
    Remove {
        name: String,
    },
    Hierarchy {
        name: String,
        scope: String,
        max_depth: usize,
        max_results: usize,
    },
    ValueAt {
        name: String,
        signal: String,
        time_steps: u64,
    },
    Changes {
        name: String,
        signal: String,
        start_time_steps: Option<u64>,
        end_time_steps: Option<u64>,
        cursor: Option<u64>,
        limit: usize,
    },
    Snapshot {
        name: String,
        scope: String,
        time_steps: u64,
        max_signals: usize,
    },
    Summary,
}

struct RequestEnvelope {
    request: TraceRequest,
    response: Sender<Result<TraceResponse, DebugError>>,
    deadline: Instant,
    state: Arc<AtomicU8>,
}

#[derive(Clone)]
pub(crate) struct TraceClient {
    requests: SyncSender<RequestEnvelope>,
}

impl TraceClient {
    pub(crate) fn request_timeout(
        &self,
        request: TraceRequest,
        timeout: Duration,
    ) -> Result<TraceResponse, DebugError> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| DebugError::new("FST request timeout is too large"))?;
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
                TrySendError::Full(_) => DebugError::new("FST request queue is full"),
                TrySendError::Disconnected(_) => {
                    DebugError::new("FST recording session is no longer running")
                }
            })?;

        match response_rx.recv_timeout(timeout) {
            Ok(result) => result,
            Err(RecvTimeoutError::Timeout) => match state.compare_exchange(
                REQUEST_PENDING,
                REQUEST_CANCELLED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) | Err(REQUEST_CANCELLED) => Err(DebugError::new(
                    "FST request timed out waiting for simulator poll",
                )),
                Err(REQUEST_CLAIMED) => response_rx.recv().unwrap_or_else(|_| {
                    Err(DebugError::new(
                        "FST session stopped while executing a claimed request",
                    ))
                }),
                Err(state) => Err(DebugError::new(format!(
                    "FST request entered invalid state {state}"
                ))),
            },
            Err(RecvTimeoutError::Disconnected) => Err(DebugError::new(
                "FST session stopped before returning a response",
            )),
        }
    }
}

trait TraceHost {
    fn status(&mut self) -> Result<verilator_trace::TraceStatus, verilator_trace::TraceError>;
    fn start(
        &mut self,
        path: &Path,
    ) -> Result<verilator_trace::TraceStatus, verilator_trace::TraceError>;
    fn flush(&mut self) -> Result<verilator_trace::TraceStatus, verilator_trace::TraceError>;
    fn stop(&mut self) -> Result<verilator_trace::TraceStatus, verilator_trace::TraceError>;
}

struct RustdvTraceHost;

impl TraceHost for RustdvTraceHost {
    fn status(&mut self) -> Result<verilator_trace::TraceStatus, verilator_trace::TraceError> {
        verilator_trace::status()
    }

    fn start(
        &mut self,
        path: &Path,
    ) -> Result<verilator_trace::TraceStatus, verilator_trace::TraceError> {
        verilator_trace::start(path)
    }

    fn flush(&mut self) -> Result<verilator_trace::TraceStatus, verilator_trace::TraceError> {
        verilator_trace::flush()
    }

    fn stop(&mut self) -> Result<verilator_trace::TraceStatus, verilator_trace::TraceError> {
        verilator_trace::stop()
    }
}

struct TraceRecording {
    spec: RecordingSpec,
    active: bool,
    closed_segments: Vec<PathBuf>,
    active_path: Option<PathBuf>,
    start_time_steps: u64,
    end_time_steps: u64,
    bytes: u64,
    signal_count: usize,
    retained: usize,
    dropped: u64,
    next_cursor: u64,
    read_errors: u64,
    last_error: Option<String>,
    limit_reached: bool,
    limit_reason: Option<String>,
    last_limit_check_time_steps: u64,
    projection_through_time_steps: Option<u64>,
    projection_samples: VecDeque<RecordingSample>,
    projection_values: Vec<Option<String>>,
    projection_materialized_segments: usize,
    index: Option<TraceIndex>,
}

impl TraceRecording {
    fn paths(&self) -> impl Iterator<Item = &Path> {
        self.closed_segments
            .iter()
            .map(PathBuf::as_path)
            .chain(self.active_path.iter().map(PathBuf::as_path))
    }

    fn query_paths(&self) -> impl Iterator<Item = &Path> {
        self.closed_segments.iter().map(PathBuf::as_path)
    }

    fn status(&self) -> TraceRecordingStatus {
        TraceRecordingStatus {
            spec: self.spec.clone(),
            active: self.active,
            retained: self.retained,
            dropped: self.dropped,
            next_cursor: self.next_cursor,
            read_errors: self.read_errors,
            last_error: self.last_error.clone(),
            backend: "fst".to_owned(),
            start_time_steps: self.start_time_steps,
            end_time_steps: self.end_time_steps,
            bytes: self.bytes,
            signal_count: self.signal_count,
            segments: self.closed_segments.len() + usize::from(self.active_path.is_some()),
            limit_reached: self.limit_reached,
            limit_reason: self.limit_reason.clone(),
            projection_through_time_steps: self.projection_through_time_steps,
        }
    }
}

#[derive(Clone, Debug)]
struct IndexedSignal {
    path: String,
    width: u32,
    handle_index: usize,
}

#[derive(Clone)]
struct TraceIndex {
    hierarchy: Vec<SignalInfo>,
    signals: Vec<IndexedSignal>,
    exact: HashMap<String, usize>,
}

impl TraceIndex {
    fn read(path: &Path, max_entries: usize, max_path_bytes: usize) -> Result<Self, DebugError> {
        let mut reader = open_reader(path)?;
        let mut stack = Vec::<String>::new();
        let mut hierarchy = Vec::new();
        let mut signals = Vec::new();
        let mut exact = HashMap::new();
        let mut limit_error = None;
        reader
            .read_hierarchy(|entry| {
                if limit_error.is_some() {
                    return;
                }
                match entry {
                    FstHierarchyEntry::Scope { name, .. } => {
                        let name = if name == "$rootio" {
                            "TOP".to_owned()
                        } else {
                            name
                        };
                        let path_bytes =
                            stack.iter().map(String::len).sum::<usize>() + stack.len() + name.len();
                        if path_bytes > max_path_bytes {
                            limit_error = Some(DebugError::new(format!(
                                "captured hierarchy path exceeds {max_path_bytes} bytes"
                            )));
                            return;
                        }
                        if hierarchy.len() >= max_entries {
                            limit_error = Some(DebugError::new(format!(
                                "captured hierarchy exceeds {max_entries} indexed entries"
                            )));
                            return;
                        }
                        stack.push(name);
                        hierarchy.push(SignalInfo {
                            path: stack.join("."),
                            kind: SignalKind::Scope,
                            width: None,
                        });
                    }
                    FstHierarchyEntry::UpScope => {
                        stack.pop();
                    }
                    FstHierarchyEntry::Var {
                        name,
                        length,
                        handle,
                        ..
                    } => {
                        let name = name
                            .rsplit_once(" [")
                            .filter(|(_, range)| range.ends_with(']'))
                            .map_or(name.as_str(), |(base, _)| base)
                            .to_owned();
                        let path_bytes =
                            stack.iter().map(String::len).sum::<usize>() + stack.len() + name.len();
                        if path_bytes > max_path_bytes {
                            limit_error = Some(DebugError::new(format!(
                                "captured hierarchy path exceeds {max_path_bytes} bytes"
                            )));
                            return;
                        }
                        if hierarchy.len() >= max_entries {
                            limit_error = Some(DebugError::new(format!(
                                "captured hierarchy exceeds {max_entries} indexed entries"
                            )));
                            return;
                        }
                        let path = if stack.is_empty() {
                            name
                        } else {
                            format!("{}.{}", stack.join("."), name)
                        };
                        hierarchy.push(SignalInfo {
                            path: path.clone(),
                            kind: SignalKind::Logic,
                            width: Some(length),
                        });
                        let signal_index = signals.len();
                        exact.entry(path.clone()).or_insert(signal_index);
                        signals.push(IndexedSignal {
                            path,
                            width: length,
                            handle_index: handle.get_index(),
                        });
                    }
                    _ => {}
                }
            })
            .map_err(|error| DebugError::new(format!("cannot read FST hierarchy: {error}")))?;
        if let Some(error) = limit_error {
            return Err(error);
        }
        Ok(Self {
            hierarchy,
            signals,
            exact,
        })
    }

    fn resolve(&self, requested: &str) -> Result<&IndexedSignal, DebugError> {
        let requested = requested.trim().trim_matches('.');
        if requested.is_empty() || requested.contains('\0') {
            return Err(DebugError::new("trace signal path is invalid"));
        }
        if let Some(index) = self.exact.get(requested) {
            return Ok(&self.signals[*index]);
        }
        if let Some(without_top) = requested.strip_prefix("TOP.") {
            if let Some(index) = self.exact.get(without_top) {
                return Ok(&self.signals[*index]);
            }
        } else if let Some(index) = self.exact.get(&format!("TOP.{requested}")) {
            return Ok(&self.signals[*index]);
        }

        let suffix = format!(".{requested}");
        let matches: Vec<_> = self
            .signals
            .iter()
            .filter(|signal| signal.path == requested || signal.path.ends_with(&suffix))
            .collect();
        let preferred: Vec<_> = matches
            .iter()
            .copied()
            .filter(|signal| !signal.path.starts_with("$rootio."))
            .collect();
        let matches = if preferred.is_empty() {
            matches
        } else {
            preferred
        };
        let Some(first) = matches.first().copied() else {
            return Err(DebugError::new(format!(
                "signal '{requested}' is not present in the captured trace"
            )));
        };
        if matches
            .iter()
            .skip(1)
            .any(|candidate| candidate.handle_index != first.handle_index)
        {
            return Err(DebugError::new(format!(
                "signal path '{requested}' is ambiguous; use its full captured hierarchy path"
            )));
        }
        Ok(first)
    }

    fn hierarchy(&self, scope: &str, max_depth: usize, max_results: usize) -> Vec<SignalInfo> {
        let scope = scope.trim().trim_matches('.');
        let prefix = (!scope.is_empty()).then(|| format!("{scope}."));
        self.hierarchy
            .iter()
            .filter(|entry| {
                let relative = if scope.is_empty() {
                    entry.path.as_str()
                } else if entry.path == scope {
                    return false;
                } else if let Some(relative) = entry.path.strip_prefix(prefix.as_ref().unwrap()) {
                    relative
                } else {
                    return false;
                };
                relative.matches('.').count() <= max_depth
            })
            .take(max_results)
            .cloned()
            .collect()
    }

    fn signals_in_scope(&self, scope: &str) -> Vec<&IndexedSignal> {
        let scope = scope.trim().trim_matches('.');
        if scope.is_empty() {
            return self.signals.iter().collect();
        }
        let prefix = format!("{scope}.");
        self.signals
            .iter()
            .filter(|signal| signal.path.starts_with(&prefix))
            .collect()
    }
}

pub(crate) struct TraceSession {
    requests: Receiver<RequestEnvelope>,
    config: TraceRecordingConfig,
    recordings: BTreeMap<String, TraceRecording>,
    temporary_directory: Option<TempDir>,
    next_file_id: u64,
    host: Box<dyn TraceHost>,
}

impl TraceSession {
    pub(crate) fn new(config: TraceRecordingConfig) -> Result<(Self, TraceClient), DebugError> {
        Self::with_host(config, Box::new(RustdvTraceHost))
    }

    fn with_host(
        config: TraceRecordingConfig,
        host: Box<dyn TraceHost>,
    ) -> Result<(Self, TraceClient), DebugError> {
        let config = config.validate()?;
        let (requests, receiver) = mpsc::sync_channel(config.request_queue_capacity);
        Ok((
            Self {
                requests: receiver,
                config,
                recordings: BTreeMap::new(),
                temporary_directory: None,
                next_file_id: 0,
                host,
            },
            TraceClient { requests },
        ))
    }

    pub(crate) fn poll(&mut self, simulation_time_steps: u64) {
        self.enforce_active_limits(simulation_time_steps);
        for _ in 0..self.config.max_requests_per_poll {
            let envelope = match self.requests.try_recv() {
                Ok(envelope) => envelope,
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            };
            self.service_envelope(envelope, simulation_time_steps);
        }
    }

    pub(crate) fn next_deadline_steps(&self) -> Option<u64> {
        let recording = self
            .recordings
            .values()
            .find(|recording| recording.active)?;
        let duration_deadline = recording
            .start_time_steps
            .saturating_add(self.config.max_duration_steps);
        let byte_check_deadline = recording
            .last_limit_check_time_steps
            .saturating_add(self.config.limit_check_interval_steps);
        Some(duration_deadline.min(byte_check_deadline))
    }

    pub(crate) fn stop_active(&mut self, simulation_time_steps: u64) {
        if let Some(name) = self.active_name() {
            let _ = self.stop_recording(&name, simulation_time_steps, None);
        }
    }

    fn service_envelope(&mut self, envelope: RequestEnvelope, simulation_time_steps: u64) {
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
        let result = self.handle(envelope.request, simulation_time_steps);
        let _ = envelope.response.send(result);
    }

    fn handle(
        &mut self,
        request: TraceRequest,
        simulation_time_steps: u64,
    ) -> Result<TraceResponse, DebugError> {
        match request {
            TraceRequest::Start {
                name,
                signals,
                capacity,
            } => self.start_recording(name, signals, capacity, simulation_time_steps),
            TraceRequest::Status { name } => {
                self.refresh_bytes(&name)?;
                Ok(TraceResponse::RecordingStatus(
                    self.recording(&name)?.status(),
                ))
            }
            TraceRequest::Get {
                name,
                cursor,
                limit,
            } => {
                self.prepare_query(&name, simulation_time_steps)?;
                self.get_recording(&name, cursor, limit)
            }
            TraceRequest::Stop { name } => {
                self.stop_recording(&name, simulation_time_steps, None)?;
                Ok(TraceResponse::RecordingStatus(
                    self.recording(&name)?.status(),
                ))
            }
            TraceRequest::Remove { name } => self.remove_recording(&name, simulation_time_steps),
            TraceRequest::Hierarchy {
                name,
                scope,
                max_depth,
                max_results,
            } => {
                self.prepare_query(&name, simulation_time_steps)?;
                let max_results = max_results.clamp(1, self.config.max_hierarchy_results);
                let index = self.index(&name)?;
                Ok(TraceResponse::Hierarchy(index.hierarchy(
                    &scope,
                    max_depth.min(32),
                    max_results,
                )))
            }
            TraceRequest::ValueAt {
                name,
                signal,
                time_steps,
            } => {
                self.prepare_query(&name, simulation_time_steps)?;
                let result = self.value_at(&name, &signal, time_steps);
                if let Err(error) = &result {
                    self.note_read_error(&name, error);
                }
                result.map(TraceResponse::ValueAt)
            }
            TraceRequest::Changes {
                name,
                signal,
                start_time_steps,
                end_time_steps,
                cursor,
                limit,
            } => {
                self.prepare_query(&name, simulation_time_steps)?;
                let result = self.changes(
                    &name,
                    &signal,
                    start_time_steps,
                    end_time_steps,
                    cursor,
                    limit,
                );
                if let Err(error) = &result {
                    self.note_read_error(&name, error);
                }
                result.map(TraceResponse::Changes)
            }
            TraceRequest::Snapshot {
                name,
                scope,
                time_steps,
                max_signals,
            } => {
                self.prepare_query(&name, simulation_time_steps)?;
                let result = self.snapshot(&name, &scope, time_steps, max_signals);
                if let Err(error) = &result {
                    self.note_read_error(&name, error);
                }
                result.map(TraceResponse::Snapshot)
            }
            TraceRequest::Summary => self.summary().map(TraceResponse::Summary),
        }
    }

    fn start_recording(
        &mut self,
        name: String,
        signals: Vec<String>,
        capacity: usize,
        simulation_time_steps: u64,
    ) -> Result<TraceResponse, DebugError> {
        let name = name.trim().to_owned();
        if name.is_empty() || name.contains('\0') {
            return Err(DebugError::new("recording name is invalid"));
        }
        if signals.len() > self.config.max_signals_per_recording {
            return Err(DebugError::new(format!(
                "recording may project at most {} signals",
                self.config.max_signals_per_recording
            )));
        }
        if signals
            .iter()
            .any(|path| path.trim().is_empty() || path.contains('\0'))
        {
            return Err(DebugError::new("recording signal path is invalid"));
        }
        if signals.iter().collect::<HashSet<_>>().len() != signals.len() {
            return Err(DebugError::new("recording signal paths must be unique"));
        }
        if capacity == 0 || capacity > self.config.max_recording_capacity {
            return Err(DebugError::new(format!(
                "recording capacity must be between 1 and {}",
                self.config.max_recording_capacity
            )));
        }
        if self.active_name().is_some() {
            return Err(DebugError::new(
                "only one all-signal FST recording may be active at a time",
            ));
        }
        if !self.recordings.contains_key(&name)
            && self.recordings.len() >= self.config.max_recordings
        {
            return Err(DebugError::new(format!(
                "at most {} FST recordings may be retained",
                self.config.max_recordings
            )));
        }
        if self.recordings.contains_key(&name) {
            return Err(DebugError::new(format!(
                "recording '{name}' already exists; remove it before reusing the name"
            )));
        }

        self.host
            .status()
            .map_err(|error| DebugError::new(format!("cannot arm FST capture: {error}")))?;
        let path = self.next_path()?;
        let host_status = self
            .host
            .start(&path)
            .map_err(|error| DebugError::new(format!("cannot arm FST capture: {error}")))?;
        let has_projection = !signals.is_empty();
        let projection_signal_count = signals.len();
        let recording = TraceRecording {
            spec: RecordingSpec {
                name: name.clone(),
                signals,
                capacity,
            },
            active: true,
            closed_segments: Vec::new(),
            active_path: Some(path),
            start_time_steps: host_status.start_time_steps,
            end_time_steps: host_status.end_time_steps,
            bytes: 0,
            signal_count: 0,
            retained: 0,
            dropped: 0,
            next_cursor: 0,
            read_errors: 0,
            last_error: None,
            limit_reached: false,
            limit_reason: None,
            last_limit_check_time_steps: simulation_time_steps,
            projection_through_time_steps: None,
            projection_samples: VecDeque::new(),
            projection_values: vec![None; projection_signal_count],
            projection_materialized_segments: 0,
            index: None,
        };
        self.recordings.insert(name.clone(), recording);
        if has_projection {
            let initialized = (|| {
                self.rotate_active_segment(&name, simulation_time_steps)?;
                self.ensure_index(&name)?;
                self.validate_projection(&name)?;
                self.refresh_projection_stats(&name)?;
                Ok(self.recording(&name)?.status())
            })();
            return match initialized {
                Ok(status) => Ok(TraceResponse::RecordingStatus(status)),
                Err(error) => match self.discard_recording(&name) {
                    Ok(()) => Err(error),
                    Err(cleanup_error) => Err(DebugError::new(format!(
                        "{error}; failed to discard incomplete recording: {cleanup_error}"
                    ))),
                },
            };
        }
        Ok(TraceResponse::RecordingStatus(
            self.recording(&name)?.status(),
        ))
    }

    fn stop_recording(
        &mut self,
        name: &str,
        simulation_time_steps: u64,
        limit_reason: Option<String>,
    ) -> Result<(), DebugError> {
        if !self.recording(name)?.active {
            return Ok(());
        }
        let status = self
            .host
            .stop()
            .map_err(|error| DebugError::new(error.to_string()))?;
        let recording = self.recording_mut(name)?;
        if let Some(path) = recording.active_path.take() {
            recording.closed_segments.push(path);
        }
        recording.active = false;
        recording.end_time_steps = status.end_time_steps.max(simulation_time_steps);
        if let Some(reason) = limit_reason {
            recording.limit_reached = true;
            recording.limit_reason = Some(reason);
        }
        let has_projection = !self.recording(name)?.spec.signals.is_empty();
        if let Err(error) = (|| {
            self.refresh_bytes(name)?;
            if has_projection {
                self.ensure_index(name)?;
                self.refresh_projection_stats(name)?;
            }
            Ok::<(), DebugError>(())
        })() {
            self.note_read_error(name, &error);
        }
        Ok(())
    }

    fn remove_recording(
        &mut self,
        name: &str,
        simulation_time_steps: u64,
    ) -> Result<TraceResponse, DebugError> {
        if self
            .recordings
            .get(name)
            .is_some_and(|recording| recording.active)
        {
            self.stop_recording(name, simulation_time_steps, None)?;
        }
        if !self.recordings.contains_key(name) {
            return Ok(TraceResponse::Removed(false));
        }
        self.delete_recording_files(name)?;
        self.recordings.remove(name);
        Ok(TraceResponse::Removed(true))
    }

    fn discard_recording(&mut self, name: &str) -> Result<(), DebugError> {
        if self
            .recordings
            .get(name)
            .is_some_and(|recording| recording.active)
        {
            let status = self
                .host
                .stop()
                .map_err(|error| DebugError::new(error.to_string()))?;
            let recording = self.recording_mut(name)?;
            if let Some(path) = recording.active_path.take() {
                recording.closed_segments.push(path);
            }
            recording.active = false;
            recording.end_time_steps = status.end_time_steps;
        }
        self.delete_recording_files(name)?;
        self.recordings.remove(name);
        Ok(())
    }

    fn delete_recording_files(&mut self, name: &str) -> Result<(), DebugError> {
        let paths: Vec<_> = self
            .recording(name)?
            .paths()
            .map(Path::to_path_buf)
            .collect();
        for path in paths {
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    let message =
                        format!("cannot delete private FST '{}': {error}", path.display());
                    let recording = self.recording_mut(name)?;
                    recording.last_error = Some(message.clone());
                    return Err(DebugError::new(message));
                }
            }
        }
        Ok(())
    }

    fn prepare_query(&mut self, name: &str, simulation_time_steps: u64) -> Result<(), DebugError> {
        let result = (|| {
            if self.recording(name)?.active {
                self.rotate_active_segment(name, simulation_time_steps)?;
            }
            self.ensure_index(name)?;
            if !self.recording(name)?.spec.signals.is_empty() {
                self.refresh_projection_stats(name)?;
            }
            Ok(())
        })();
        if let Err(error) = &result {
            self.note_read_error(name, error);
        }
        result
    }

    fn rotate_active_segment(
        &mut self,
        name: &str,
        simulation_time_steps: u64,
    ) -> Result<(), DebugError> {
        let segments = {
            let recording = self.recording(name)?;
            recording.closed_segments.len() + usize::from(recording.active_path.is_some())
        };
        if segments >= self.config.max_segments_per_recording {
            let reason = format!(
                "maximum of {} query segments reached",
                self.config.max_segments_per_recording
            );
            self.stop_recording(name, simulation_time_steps, Some(reason.clone()))?;
            return Err(DebugError::new(reason));
        }

        let stopped = self
            .host
            .stop()
            .map_err(|error| DebugError::new(error.to_string()))?;
        {
            let recording = self.recording_mut(name)?;
            if let Some(path) = recording.active_path.take() {
                recording.closed_segments.push(path);
            }
            recording.end_time_steps = stopped.end_time_steps;
            recording.index = None;
        }

        let next_path = self.next_path()?;
        match self.host.start(&next_path) {
            Ok(started) => {
                let recording = self.recording_mut(name)?;
                recording.active_path = Some(next_path);
                recording.end_time_steps = started.end_time_steps;
                self.refresh_bytes(name)?;
                Ok(())
            }
            Err(error) => {
                let cleanup_error = match std::fs::remove_file(&next_path) {
                    Ok(()) => None,
                    Err(cleanup) if cleanup.kind() == std::io::ErrorKind::NotFound => None,
                    Err(cleanup) => Some(cleanup),
                };
                let recording = self.recording_mut(name)?;
                recording.active = false;
                let message = if let Some(cleanup) = cleanup_error {
                    recording.closed_segments.push(next_path);
                    format!(
                        "capture stopped while rotating a query segment: {error}; cannot delete failed private FST: {cleanup}"
                    )
                } else {
                    format!("capture stopped while rotating a query segment: {error}")
                };
                recording.last_error = Some(message.clone());
                Err(DebugError::new(message))
            }
        }
    }

    fn enforce_active_limits(&mut self, simulation_time_steps: u64) {
        let Some(name) = self.active_name() else {
            return;
        };
        let duration_exceeded = self.recording(&name).is_ok_and(|recording| {
            simulation_time_steps.saturating_sub(recording.start_time_steps)
                >= self.config.max_duration_steps
        });
        if duration_exceeded {
            let reason = format!(
                "maximum recording duration of {} simulation steps reached",
                self.config.max_duration_steps
            );
            let _ = self.stop_recording(&name, simulation_time_steps, Some(reason));
            return;
        }

        let should_check_bytes = self.recording(&name).is_ok_and(|recording| {
            simulation_time_steps.saturating_sub(recording.last_limit_check_time_steps)
                >= self.config.limit_check_interval_steps
        });
        if !should_check_bytes {
            return;
        }
        match self.host.flush() {
            Ok(status) => {
                if let Ok(recording) = self.recording_mut(&name) {
                    recording.end_time_steps = status.end_time_steps;
                    recording.last_limit_check_time_steps = simulation_time_steps;
                }
                let _ = self.refresh_bytes(&name);
                let bytes_exceeded = self
                    .recording(&name)
                    .is_ok_and(|recording| recording.bytes >= self.config.max_bytes_per_recording);
                if bytes_exceeded {
                    let reason = format!(
                        "maximum recording size of {} bytes reached",
                        self.config.max_bytes_per_recording
                    );
                    let _ = self.stop_recording(&name, simulation_time_steps, Some(reason));
                }
            }
            Err(error) => {
                let reason = format!("FST flush failed while enforcing capture limits: {error}");
                let stop_result =
                    self.stop_recording(&name, simulation_time_steps, Some(reason.clone()));
                if let Ok(recording) = self.recording_mut(&name) {
                    recording.last_error = Some(match stop_result {
                        Ok(()) => reason,
                        Err(stop_error) => {
                            format!("{reason}; capture stop also failed: {stop_error}")
                        }
                    });
                }
            }
        }
    }

    fn summary(&mut self) -> Result<TraceSummary, DebugError> {
        let available = match self.host.status() {
            Ok(status) => status.state != verilator_trace::TraceState::Unavailable,
            Err(verilator_trace::TraceError::Unavailable(_)) => false,
            Err(error) => return Err(DebugError::new(error.to_string())),
        };
        for recording in self.recordings.values_mut() {
            recording.bytes = recording
                .paths()
                .filter_map(|path| std::fs::metadata(path).ok())
                .map(|metadata| metadata.len())
                .sum();
        }
        Ok(TraceSummary {
            backend: "fst".to_owned(),
            available,
            recordings: self.recordings.len(),
            active_recording: self.active_name(),
            retained_bytes: self
                .recordings
                .values()
                .map(|recording| recording.bytes)
                .sum(),
            max_bytes_per_recording: self.config.max_bytes_per_recording,
            max_duration_steps: self.config.max_duration_steps,
            max_recordings: self.config.max_recordings,
            max_signals_per_recording: self.config.max_signals_per_recording,
            max_recording_capacity: self.config.max_recording_capacity,
            max_recording_page_size: self.config.max_recording_page_size,
            max_hierarchy_results: self.config.max_hierarchy_results,
            max_indexed_hierarchy_entries: self.config.max_indexed_hierarchy_entries,
            max_hierarchy_path_bytes: self.config.max_hierarchy_path_bytes,
            max_snapshot_signals: self.config.max_snapshot_signals,
            max_changes_per_query: self.config.max_changes_per_query,
            max_changes_scanned: self.config.max_changes_scanned,
            max_decoded_bytes_per_query: self.config.max_decoded_bytes_per_query,
            max_projected_value_bytes: self.config.max_projected_value_bytes,
            max_segments_per_recording: self.config.max_segments_per_recording,
            limit_check_interval_steps: self.config.limit_check_interval_steps,
        })
    }

    fn get_recording(
        &mut self,
        name: &str,
        cursor: Option<u64>,
        limit: usize,
    ) -> Result<TraceResponse, DebugError> {
        let recording = self.recording(name)?;
        if recording.spec.signals.is_empty() {
            return Err(DebugError::new(
                "this all-signal recording has no default projection; use recording_hierarchy and filtered history tools",
            ));
        }
        let samples = &recording.projection_samples;
        let total = recording.next_cursor;
        let dropped = recording.dropped;
        let oldest = samples.front().map_or(total, |sample| sample.cursor);
        let requested = cursor.unwrap_or(oldest);
        let truncated = requested < oldest;
        let start_cursor = requested.max(oldest);
        let limit = limit.clamp(1, self.config.max_recording_page_size);
        let page: Vec<_> = samples
            .iter()
            .filter(|sample| sample.cursor >= start_cursor)
            .take(limit)
            .cloned()
            .collect();
        let next_cursor = page.last().and_then(|sample| {
            let next = sample.cursor.saturating_add(1);
            (next < total).then_some(next)
        });
        Ok(TraceResponse::Recording(RecordingPage {
            name: name.to_owned(),
            samples: page,
            next_cursor,
            dropped,
            truncated,
        }))
    }

    fn refresh_projection_stats(&mut self, name: &str) -> Result<(), DebugError> {
        if self.recording(name)?.spec.signals.is_empty() {
            return Ok(());
        }
        let mut recording = self
            .recordings
            .remove(name)
            .ok_or_else(|| DebugError::new(format!("recording '{name}' does not exist")))?;
        let result = (|| {
            let index = recording
                .index
                .as_ref()
                .ok_or_else(|| DebugError::new("recording hierarchy has not been indexed"))?;
            let selected = resolve_projection(index, &recording.spec.signals)?;
            let paths: Vec<_> = recording
                .closed_segments
                .iter()
                .skip(recording.projection_materialized_segments)
                .cloned()
                .collect();
            append_projection_segments(
                &paths,
                &selected,
                &mut recording.projection_values,
                &mut recording.projection_samples,
                &mut recording.next_cursor,
                &mut recording.dropped,
                recording.spec.capacity,
                self.config.max_projected_value_bytes,
            )?;
            recording.projection_materialized_segments = recording.closed_segments.len();
            recording.retained = recording.projection_samples.len();
            recording.projection_through_time_steps = Some(recording.end_time_steps);
            Ok(())
        })();
        self.recordings.insert(name.to_owned(), recording);
        result
    }

    fn validate_projection(&self, name: &str) -> Result<(), DebugError> {
        let recording = self.recording(name)?;
        let index = self.index(name)?;
        resolve_projection(index, &recording.spec.signals)?;
        Ok(())
    }

    fn value_at(
        &mut self,
        name: &str,
        requested: &str,
        time_steps: u64,
    ) -> Result<TraceValueAt, DebugError> {
        let (start, end, paths) = self.query_window(name)?;
        if time_steps < start || time_steps > end {
            return Err(DebugError::new(format!(
                "requested time {time_steps} is outside capture window {start}..={end}"
            )));
        }
        let signal = self.index(name)?.resolve(requested)?.clone();
        let events = read_events(
            &paths,
            std::slice::from_ref(&signal),
            start,
            time_steps,
            self.config.max_changes_scanned,
            self.config.max_decoded_bytes_per_query,
        )?;
        let event = events.last().ok_or_else(|| {
            DebugError::new(format!(
                "signal '{}' has no captured value at or before {time_steps}",
                signal.path
            ))
        })?;
        Ok(TraceValueAt {
            name: name.to_owned(),
            requested_time_steps: time_steps,
            sampled_time_steps: event.time_steps,
            value: signal_value(&signal, &event.value),
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn changes(
        &mut self,
        name: &str,
        requested: &str,
        start_time_steps: Option<u64>,
        end_time_steps: Option<u64>,
        cursor: Option<u64>,
        limit: usize,
    ) -> Result<TraceChangePage, DebugError> {
        let (capture_start, capture_end, paths) = self.query_window(name)?;
        let start = start_time_steps.unwrap_or(capture_start).max(capture_start);
        let end = end_time_steps.unwrap_or(capture_end).min(capture_end);
        if start > end {
            return Err(DebugError::new(
                "trace change query has an empty time range",
            ));
        }
        let signal = self.index(name)?.resolve(requested)?.clone();
        let events = read_events(
            &paths,
            std::slice::from_ref(&signal),
            start,
            end,
            self.config.max_changes_scanned,
            self.config.max_decoded_bytes_per_query,
        )?;
        let cursor = cursor.unwrap_or(0) as usize;
        let limit = limit.clamp(1, self.config.max_changes_per_query);
        let changes: Vec<_> = events
            .iter()
            .enumerate()
            .skip(cursor)
            .take(limit)
            .map(|(index, event)| TraceChange {
                cursor: index as u64,
                simulation_time_steps: event.time_steps,
                value: signal_value(&signal, &event.value),
            })
            .collect();
        let next_cursor = changes.last().and_then(|change| {
            let next = change.cursor.saturating_add(1);
            (next < events.len() as u64).then_some(next)
        });
        Ok(TraceChangePage {
            name: name.to_owned(),
            signal: signal.path,
            changes,
            next_cursor,
            truncated: cursor > 0,
        })
    }

    fn snapshot(
        &mut self,
        name: &str,
        scope: &str,
        time_steps: u64,
        max_signals: usize,
    ) -> Result<TraceSnapshot, DebugError> {
        let (start, end, paths) = self.query_window(name)?;
        if time_steps < start || time_steps > end {
            return Err(DebugError::new(format!(
                "requested time {time_steps} is outside capture window {start}..={end}"
            )));
        }
        let max_signals = max_signals.clamp(1, self.config.max_snapshot_signals);
        let index = self.index(name)?.clone();
        let in_scope = index.signals_in_scope(scope);
        let truncated = in_scope.len() > max_signals;
        let selected: Vec<_> = in_scope.into_iter().take(max_signals).cloned().collect();
        let events = read_events(
            &paths,
            &selected,
            start,
            time_steps,
            self.config.max_changes_scanned,
            self.config.max_decoded_bytes_per_query,
        )?;
        let mut latest = HashMap::new();
        for event in events {
            latest.insert(event.handle_index, event.value);
        }
        let values = selected
            .iter()
            .filter_map(|signal| {
                latest
                    .get(&signal.handle_index)
                    .map(|value| signal_value(signal, value))
            })
            .collect();
        Ok(TraceSnapshot {
            name: name.to_owned(),
            scope: scope.to_owned(),
            requested_time_steps: time_steps,
            values,
            truncated,
        })
    }

    fn query_window(&self, name: &str) -> Result<(u64, u64, Vec<PathBuf>), DebugError> {
        let recording = self.recording(name)?;
        Ok((
            recording.start_time_steps,
            recording.end_time_steps,
            recording.query_paths().map(Path::to_path_buf).collect(),
        ))
    }

    fn ensure_index(&mut self, name: &str) -> Result<(), DebugError> {
        if self.recording(name)?.index.is_some() {
            return Ok(());
        }
        let path = self
            .recording(name)?
            .closed_segments
            .first()
            .cloned()
            .ok_or_else(|| DebugError::new("recording has no closed FST segment to query"))?;
        let index = TraceIndex::read(
            &path,
            self.config.max_indexed_hierarchy_entries,
            self.config.max_hierarchy_path_bytes,
        )?;
        let signal_count = index.signals.len();
        let recording = self.recording_mut(name)?;
        recording.signal_count = signal_count;
        recording.index = Some(index);
        Ok(())
    }

    fn index(&self, name: &str) -> Result<&TraceIndex, DebugError> {
        self.recording(name)?
            .index
            .as_ref()
            .ok_or_else(|| DebugError::new("recording hierarchy has not been indexed"))
    }

    fn refresh_bytes(&mut self, name: &str) -> Result<(), DebugError> {
        let bytes = self
            .recording(name)?
            .paths()
            .filter_map(|path| std::fs::metadata(path).ok())
            .map(|metadata| metadata.len())
            .sum();
        self.recording_mut(name)?.bytes = bytes;
        Ok(())
    }

    fn note_read_error(&mut self, name: &str, error: &DebugError) {
        if let Ok(recording) = self.recording_mut(name) {
            recording.read_errors = recording.read_errors.saturating_add(1);
            recording.last_error = Some(error.to_string());
        }
    }

    fn recording(&self, name: &str) -> Result<&TraceRecording, DebugError> {
        self.recordings
            .get(name)
            .ok_or_else(|| DebugError::new(format!("recording '{name}' does not exist")))
    }

    fn recording_mut(&mut self, name: &str) -> Result<&mut TraceRecording, DebugError> {
        self.recordings
            .get_mut(name)
            .ok_or_else(|| DebugError::new(format!("recording '{name}' does not exist")))
    }

    fn active_name(&self) -> Option<String> {
        self.recordings
            .iter()
            .find_map(|(name, recording)| recording.active.then(|| name.clone()))
    }

    fn next_path(&mut self) -> Result<PathBuf, DebugError> {
        if self.temporary_directory.is_none() {
            self.temporary_directory = Some(
                tempfile::Builder::new()
                    .prefix("rustdv-mcp-fst-")
                    .tempdir()
                    .map_err(|error| {
                        DebugError::new(format!("cannot create private FST directory: {error}"))
                    })?,
            );
        }
        let id = self.next_file_id;
        self.next_file_id = self.next_file_id.saturating_add(1);
        Ok(self
            .temporary_directory
            .as_ref()
            .expect("trace directory was initialized above")
            .path()
            .join(format!("segment-{id}.fst")))
    }
}

impl Drop for TraceSession {
    fn drop(&mut self) {
        if self.active_name().is_some() {
            let _ = self.host.stop();
        }
    }
}

#[derive(Clone)]
struct TraceEvent {
    time_steps: u64,
    handle_index: usize,
    value: String,
}

fn open_reader(path: &Path) -> Result<FstReader<BufReader<File>>, DebugError> {
    let file = File::open(path)
        .map_err(|error| DebugError::new(format!("cannot open private FST: {error}")))?;
    FstReader::open(BufReader::new(file))
        .map_err(|error| DebugError::new(format!("cannot decode private FST: {error}")))
}

fn read_events(
    paths: &[PathBuf],
    signals: &[IndexedSignal],
    start: u64,
    end: u64,
    max_events: usize,
    max_decoded_bytes: usize,
) -> Result<Vec<TraceEvent>, DebugError> {
    let handles: Vec<_> = signals
        .iter()
        .map(|signal| FstSignalHandle::from_index(signal.handle_index))
        .collect();
    let mut events = Vec::new();
    let mut decoded_bytes = 0_usize;
    for path in paths {
        let mut reader = open_reader(path)?;
        let header = reader.get_header();
        if header.end_time < start || header.start_time > end {
            continue;
        }
        let filter = FstFilter::new(
            start,
            end,
            handles
                .iter()
                .map(|h| FstSignalHandle::from_index(h.get_index()))
                .collect(),
        );
        let result = reader.read_signals(&filter, |time, handle, value| {
            if events.len() >= max_events {
                return Err(DebugError::new(format!(
                    "trace query exceeded the bounded scan limit of {max_events} changes"
                )));
            }
            let value = match value {
                FstSignalValue::String(value) => {
                    let Some(total) = decoded_bytes.checked_add(value.len()) else {
                        return Err(DebugError::new("trace query decoded-byte count overflowed"));
                    };
                    if total > max_decoded_bytes {
                        return Err(DebugError::new(format!(
                            "trace query exceeded the bounded decoded-data limit of {max_decoded_bytes} bytes"
                        )));
                    }
                    decoded_bytes = total;
                    String::from_utf8_lossy(value).into_owned()
                }
                FstSignalValue::Real(_) => {
                    return Err(DebugError::new(
                        "real-valued FST variables are not supported by the binary signal API",
                    ))
                }
            };
            events.push(TraceEvent {
                time_steps: time,
                handle_index: handle.get_index(),
                value,
            });
            Ok::<(), DebugError>(())
        });
        match result {
            Ok(()) => {}
            Err(ReadSignalsError::CallbackError(error)) => return Err(error),
            Err(ReadSignalsError::ReadError(error)) => {
                return Err(DebugError::new(format!(
                    "cannot read private FST changes: {error}"
                )))
            }
        }
    }
    events.sort_by_key(|event| event.time_steps);
    events.dedup_by(|left, right| {
        left.time_steps == right.time_steps
            && left.handle_index == right.handle_index
            && left.value == right.value
    });
    Ok(events)
}

fn signal_value(signal: &IndexedSignal, value: &str) -> SignalValue {
    SignalValue {
        path: signal.path.clone(),
        width: signal.width,
        binary: value.to_owned(),
    }
}

fn resolve_projection(
    index: &TraceIndex,
    paths: &[String],
) -> Result<Vec<IndexedSignal>, DebugError> {
    let mut handles = HashSet::new();
    paths
        .iter()
        .map(|path| {
            let signal = index.resolve(path)?.clone();
            if !handles.insert(signal.handle_index) {
                return Err(DebugError::new(format!(
                    "recording projection selects signal handle {} more than once through aliases",
                    signal.handle_index
                )));
            }
            Ok(signal)
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn append_projection_segments(
    paths: &[PathBuf],
    signals: &[IndexedSignal],
    current: &mut Vec<Option<String>>,
    samples: &mut VecDeque<RecordingSample>,
    next_cursor: &mut u64,
    dropped: &mut u64,
    capacity: usize,
    max_value_bytes: usize,
) -> Result<(), DebugError> {
    if current.len() != signals.len() {
        return Err(DebugError::new(
            "recording projection state does not match its selected signals",
        ));
    }
    let by_handle: HashMap<_, _> = signals
        .iter()
        .enumerate()
        .map(|(position, signal)| (signal.handle_index, position))
        .collect();
    let handles: Vec<_> = signals
        .iter()
        .map(|signal| FstSignalHandle::from_index(signal.handle_index))
        .collect();
    let mut working_current = current.clone();
    let mut working_samples = samples.clone();
    let mut working_next_cursor = *next_cursor;
    let mut working_dropped = *dropped;

    for path in paths {
        let mut reader = open_reader(path)?;
        let header = reader.get_header();
        let filter = FstFilter::new(
            header.start_time,
            header.end_time,
            handles
                .iter()
                .map(|handle| FstSignalHandle::from_index(handle.get_index()))
                .collect(),
        );
        let mut pending_time = None;
        let mut before = working_current.clone();
        let result = reader.read_signals(&filter, |time, handle, value| {
            if pending_time.is_some_and(|pending| pending != time) {
                append_projection_sample(
                    pending_time.expect("a differing pending time exists"),
                    signals,
                    &before,
                    &working_current,
                    &mut working_samples,
                    &mut working_next_cursor,
                    &mut working_dropped,
                    capacity,
                );
                before = working_current.clone();
            }
            if pending_time != Some(time) {
                pending_time = Some(time);
            }
            let Some(position) = by_handle.get(&handle.get_index()) else {
                return Ok::<(), DebugError>(());
            };
            let value = match value {
                FstSignalValue::String(value) => {
                    if value.len() > max_value_bytes {
                        return Err(DebugError::new(format!(
                            "projected FST value exceeds the bounded decoded-data limit of {max_value_bytes} bytes"
                        )));
                    }
                    String::from_utf8_lossy(value).into_owned()
                }
                FstSignalValue::Real(_) => {
                    return Err(DebugError::new(
                        "real-valued FST variables are not supported by the binary signal API",
                    ))
                }
            };
            working_current[*position] = Some(value);
            Ok(())
        });
        match result {
            Ok(()) => {}
            Err(ReadSignalsError::CallbackError(error)) => return Err(error),
            Err(ReadSignalsError::ReadError(error)) => {
                return Err(DebugError::new(format!(
                    "cannot read private FST projection: {error}"
                )))
            }
        }
        if let Some(time) = pending_time {
            append_projection_sample(
                time,
                signals,
                &before,
                &working_current,
                &mut working_samples,
                &mut working_next_cursor,
                &mut working_dropped,
                capacity,
            );
        }
    }

    *current = working_current;
    *samples = working_samples;
    *next_cursor = working_next_cursor;
    *dropped = working_dropped;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn append_projection_sample(
    time_steps: u64,
    signals: &[IndexedSignal],
    before: &[Option<String>],
    current: &[Option<String>],
    samples: &mut VecDeque<RecordingSample>,
    next_cursor: &mut u64,
    dropped: &mut u64,
    capacity: usize,
) {
    if current.iter().any(Option::is_none) || current == before {
        return;
    }
    let values = signals
        .iter()
        .zip(current)
        .map(|(signal, value)| signal_value(signal, value.as_deref().unwrap_or_default()))
        .collect();
    if samples.len() == capacity {
        samples.pop_front();
        *dropped = dropped.saturating_add(1);
    }
    samples.push_back(RecordingSample {
        cursor: *next_cursor,
        simulation_time_steps: time_steps,
        values,
    });
    *next_cursor = next_cursor.saturating_add(1);
}

#[cfg(test)]
mod tests {
    use super::*;

    struct UnavailableHost;

    struct FailingFlushHost;

    struct FailingStopHost;

    impl TraceHost for UnavailableHost {
        fn status(&mut self) -> Result<verilator_trace::TraceStatus, verilator_trace::TraceError> {
            Err(verilator_trace::TraceError::Unavailable(
                "FAST build".into(),
            ))
        }

        fn start(
            &mut self,
            _path: &Path,
        ) -> Result<verilator_trace::TraceStatus, verilator_trace::TraceError> {
            Err(verilator_trace::TraceError::Unavailable(
                "FAST build".into(),
            ))
        }

        fn flush(&mut self) -> Result<verilator_trace::TraceStatus, verilator_trace::TraceError> {
            unreachable!()
        }

        fn stop(&mut self) -> Result<verilator_trace::TraceStatus, verilator_trace::TraceError> {
            unreachable!()
        }
    }

    impl TraceHost for FailingFlushHost {
        fn status(&mut self) -> Result<verilator_trace::TraceStatus, verilator_trace::TraceError> {
            Ok(verilator_trace::TraceStatus {
                state: verilator_trace::TraceState::Idle,
                start_time_steps: 7,
                end_time_steps: 11,
                dump_count: 1,
            })
        }

        fn start(
            &mut self,
            _path: &Path,
        ) -> Result<verilator_trace::TraceStatus, verilator_trace::TraceError> {
            Ok(verilator_trace::TraceStatus {
                state: verilator_trace::TraceState::Active,
                start_time_steps: 7,
                end_time_steps: 7,
                dump_count: 1,
            })
        }

        fn flush(&mut self) -> Result<verilator_trace::TraceStatus, verilator_trace::TraceError> {
            Err(verilator_trace::TraceError::Host(
                "synthetic flush failure".into(),
            ))
        }

        fn stop(&mut self) -> Result<verilator_trace::TraceStatus, verilator_trace::TraceError> {
            Ok(verilator_trace::TraceStatus {
                state: verilator_trace::TraceState::Idle,
                start_time_steps: 7,
                end_time_steps: 11,
                dump_count: 1,
            })
        }
    }

    impl TraceHost for FailingStopHost {
        fn status(&mut self) -> Result<verilator_trace::TraceStatus, verilator_trace::TraceError> {
            Ok(verilator_trace::TraceStatus {
                state: verilator_trace::TraceState::Idle,
                start_time_steps: 7,
                end_time_steps: 7,
                dump_count: 0,
            })
        }

        fn start(
            &mut self,
            path: &Path,
        ) -> Result<verilator_trace::TraceStatus, verilator_trace::TraceError> {
            std::fs::write(path, b"synthetic trace")
                .map_err(|error| verilator_trace::TraceError::Host(error.to_string()))?;
            Ok(verilator_trace::TraceStatus {
                state: verilator_trace::TraceState::Active,
                start_time_steps: 7,
                end_time_steps: 7,
                dump_count: 1,
            })
        }

        fn flush(&mut self) -> Result<verilator_trace::TraceStatus, verilator_trace::TraceError> {
            unreachable!()
        }

        fn stop(&mut self) -> Result<verilator_trace::TraceStatus, verilator_trace::TraceError> {
            Err(verilator_trace::TraceError::Host(
                "synthetic stop failure".into(),
            ))
        }
    }

    #[test]
    fn fast_build_rejects_recording_without_leaving_state_or_files() {
        let (mut session, _client) =
            TraceSession::with_host(TraceRecordingConfig::default(), Box::new(UnavailableHost))
                .unwrap();
        assert!(session.temporary_directory.is_none());
        let result = session.start_recording("all".into(), Vec::new(), 16, 7);
        assert_eq!(
            result.unwrap_err().message,
            "cannot arm FST capture: FAST build"
        );
        assert!(session.recordings.is_empty());
        assert!(session.temporary_directory.is_none());
    }

    #[test]
    fn every_resource_limit_must_be_nonzero() {
        let invalid = TraceRecordingConfig {
            max_bytes_per_recording: 0,
            ..TraceRecordingConfig::default()
        };
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn a_flush_failure_stops_capture_instead_of_disabling_the_disk_bound() {
        let (mut session, _client) = TraceSession::with_host(
            TraceRecordingConfig {
                max_duration_steps: 100,
                limit_check_interval_steps: 4,
                ..TraceRecordingConfig::default()
            },
            Box::new(FailingFlushHost),
        )
        .unwrap();
        session
            .start_recording("all".into(), Vec::new(), 16, 7)
            .unwrap();

        session.enforce_active_limits(11);

        let status = session.recording("all").unwrap().status();
        assert!(!status.active);
        assert!(status.limit_reached);
        assert!(status
            .limit_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("flush failed")));
        assert!(status
            .last_error
            .as_deref()
            .is_some_and(|error| error.contains("synthetic flush failure")));
        assert_eq!(session.next_deadline_steps(), None);
    }

    #[test]
    fn remove_preserves_an_active_recording_when_backend_stop_fails() {
        let (mut session, _client) =
            TraceSession::with_host(TraceRecordingConfig::default(), Box::new(FailingStopHost))
                .unwrap();
        session
            .start_recording("all".into(), Vec::new(), 16, 7)
            .unwrap();
        let path = session
            .recording("all")
            .unwrap()
            .active_path
            .clone()
            .unwrap();
        assert!(path.exists());

        let error = session.remove_recording("all", 8).unwrap_err();

        assert!(error.message.contains("synthetic stop failure"));
        assert_eq!(session.active_name().as_deref(), Some("all"));
        assert!(session.recording("all").unwrap().active);
        assert!(path.exists());
    }

    #[test]
    fn active_capture_exposes_its_earliest_resource_deadline() {
        let (mut session, _client) = TraceSession::with_host(
            TraceRecordingConfig {
                max_duration_steps: 100,
                limit_check_interval_steps: 20,
                ..TraceRecordingConfig::default()
            },
            Box::new(UnavailableHost),
        )
        .unwrap();
        session.recordings.insert(
            "active".into(),
            TraceRecording {
                spec: RecordingSpec {
                    name: "active".into(),
                    signals: Vec::new(),
                    capacity: 16,
                },
                active: true,
                closed_segments: Vec::new(),
                active_path: None,
                start_time_steps: 7,
                end_time_steps: 7,
                bytes: 0,
                signal_count: 0,
                retained: 0,
                dropped: 0,
                next_cursor: 0,
                read_errors: 0,
                last_error: None,
                limit_reached: false,
                limit_reason: None,
                last_limit_check_time_steps: 11,
                projection_through_time_steps: None,
                projection_samples: VecDeque::new(),
                projection_values: Vec::new(),
                projection_materialized_segments: 0,
                index: None,
            },
        );

        assert_eq!(session.next_deadline_steps(), Some(31));
        session.recordings.clear();
    }

    #[test]
    fn projection_rejects_distinct_aliases_of_one_fst_handle() {
        let index = TraceIndex {
            hierarchy: Vec::new(),
            signals: vec![
                IndexedSignal {
                    path: "TOP.count".into(),
                    width: 4,
                    handle_index: 7,
                },
                IndexedSignal {
                    path: "count".into(),
                    width: 4,
                    handle_index: 7,
                },
            ],
            exact: HashMap::from([("TOP.count".into(), 0), ("count".into(), 1)]),
        };
        let error = resolve_projection(&index, &["TOP.count".into(), "count".into()])
            .expect_err("alias duplication was accepted");
        assert!(error.message.contains("more than once through aliases"));
    }

    #[test]
    fn projection_ring_keeps_recent_samples_after_many_changes() {
        let signals = vec![IndexedSignal {
            path: "TOP.count".into(),
            width: 4,
            handle_index: 7,
        }];
        let mut samples = VecDeque::new();
        let mut next_cursor = 0;
        let mut dropped = 0;
        let mut before = vec![None];

        for (time_steps, value) in ["0000", "0001", "0010", "0011", "0100"]
            .into_iter()
            .enumerate()
        {
            let current = vec![Some(value.to_owned())];
            append_projection_sample(
                time_steps as u64,
                &signals,
                &before,
                &current,
                &mut samples,
                &mut next_cursor,
                &mut dropped,
                2,
            );
            before = current;
        }

        assert_eq!(next_cursor, 5);
        assert_eq!(dropped, 3);
        assert_eq!(samples.len(), 2);
        assert_eq!(samples.front().map(|sample| sample.cursor), Some(3));
        assert_eq!(samples.back().map(|sample| sample.cursor), Some(4));
        assert_eq!(
            samples
                .back()
                .map(|sample| sample.values[0].binary.as_str()),
            Some("0100")
        );
    }

    #[test]
    fn deletion_failure_preserves_recording_and_accounting() {
        let (mut session, _client) =
            TraceSession::with_host(TraceRecordingConfig::default(), Box::new(UnavailableHost))
                .unwrap();
        let temporary = tempfile::tempdir().unwrap();
        let undeletable_as_file = temporary.path().join("segment.fst");
        std::fs::create_dir(&undeletable_as_file).unwrap();
        session.recordings.insert(
            "kept".into(),
            TraceRecording {
                spec: RecordingSpec {
                    name: "kept".into(),
                    signals: Vec::new(),
                    capacity: 16,
                },
                active: false,
                closed_segments: vec![undeletable_as_file.clone()],
                active_path: None,
                start_time_steps: 7,
                end_time_steps: 11,
                bytes: 123,
                signal_count: 0,
                retained: 0,
                dropped: 0,
                next_cursor: 0,
                read_errors: 0,
                last_error: None,
                limit_reached: false,
                limit_reason: None,
                last_limit_check_time_steps: 7,
                projection_through_time_steps: None,
                projection_samples: VecDeque::new(),
                projection_values: Vec::new(),
                projection_materialized_segments: 0,
                index: None,
            },
        );

        let error = session.remove_recording("kept", 11).unwrap_err();

        assert!(error.message.contains("cannot delete private FST"));
        let status = session.recording("kept").unwrap().status();
        assert_eq!(status.bytes, 123);
        assert!(status
            .last_error
            .as_deref()
            .is_some_and(|message| message.contains("cannot delete private FST")));
        assert!(undeletable_as_file.is_dir());
    }

    #[test]
    fn read_errors_are_reported_through_recording_status() {
        let (mut session, _client) =
            TraceSession::with_host(TraceRecordingConfig::default(), Box::new(UnavailableHost))
                .unwrap();
        session.recordings.insert(
            "history".into(),
            TraceRecording {
                spec: RecordingSpec {
                    name: "history".into(),
                    signals: Vec::new(),
                    capacity: 16,
                },
                active: false,
                closed_segments: Vec::new(),
                active_path: None,
                start_time_steps: 7,
                end_time_steps: 11,
                bytes: 0,
                signal_count: 0,
                retained: 0,
                dropped: 0,
                next_cursor: 0,
                read_errors: 0,
                last_error: None,
                limit_reached: false,
                limit_reason: None,
                last_limit_check_time_steps: 7,
                projection_through_time_steps: None,
                projection_samples: VecDeque::new(),
                projection_values: Vec::new(),
                projection_materialized_segments: 0,
                index: None,
            },
        );
        session.note_read_error("history", &DebugError::new("synthetic decode failure"));

        let status = session.recording("history").unwrap().status();
        assert_eq!(status.read_errors, 1);
        assert_eq!(
            status.last_error.as_deref(),
            Some("synthetic decode failure")
        );
    }
}
