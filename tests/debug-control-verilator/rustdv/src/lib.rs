use rustdv::prelude::*;
use rustdv_debug::{ControlMode, DebugError, DebugResponse, PauseReason, RecordingPage};
use rustdv_mcp::{run_debug_service, simulator_debug_session_with_config};
use rustdv_mcp_verilator::{
    run_verilator_debug_service, verilator_debug_session_with_config,
    verilator_debug_session_with_configs, DebugServiceExit, DebugSessionConfig, McpServer,
    McpServerConfig, TraceRecordingConfig, TraceResponse,
};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use std::{sync::mpsc, thread, time::Duration};

#[cfg(test)]
use rustdv_vpi_stubs as _;

rustdv::vpi_bootstrap!();

async fn call_mcp_tool<T: DeserializeOwned>(
    http: &reqwest::Client,
    endpoint: &str,
    request_id: &mut u64,
    name: &str,
    arguments: Value,
) -> Result<T, String> {
    *request_id += 1;
    println!("DEBUG MCP CALL {}: {name}", *request_id);
    let response = http
        .post(endpoint)
        .header("Content-Type", "application/json")
        .header("Accept", "application/json, text/event-stream")
        .json(&json!({
            "jsonrpc": "2.0",
            "id": *request_id,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments}
        }))
        .send()
        .await
        .map_err(|error| format!("MCP tool {name} request failed: {error}"))?;
    let status = response.status();
    let body: Value = response
        .json()
        .await
        .map_err(|error| format!("MCP tool {name} returned invalid JSON: {error}"))?;
    if !status.is_success() {
        return Err(format!("MCP tool {name} returned HTTP {status}: {body}"));
    }
    if let Some(error) = body.get("error") {
        return Err(format!("MCP tool {name} returned JSON-RPC error: {error}"));
    }
    let text = body["result"]["content"][0]["text"]
        .as_str()
        .ok_or_else(|| format!("MCP tool {name} returned no text content: {body}"))?;
    let decoded = serde_json::from_str(text).map_err(|error| {
        format!("MCP tool {name} returned an unexpected response: {error}: {text}")
    })?;
    println!("DEBUG MCP RETURN {}: {name}", *request_id);
    Ok(decoded)
}

async fn wait_for_pause(
    http: &reqwest::Client,
    endpoint: &str,
    request_id: &mut u64,
) -> Result<rustdv_debug::ControlStatus, String> {
    loop {
        let DebugResponse::Control(status) =
            call_mcp_tool(http, endpoint, request_id, "control_status", json!({})).await?
        else {
            return Err("control status returned the wrong response".into());
        };
        if status.mode == ControlMode::Paused {
            return Ok(status);
        }
        thread::yield_now();
    }
}

fn debug_config() -> DebugSessionConfig {
    DebugSessionConfig {
        pause_inactivity_timeout: Duration::from_secs(3),
        ..DebugSessionConfig::default()
    }
}

fn server_config() -> McpServerConfig {
    McpServerConfig {
        bind: "127.0.0.1:0".parse().unwrap(),
        request_timeout: Duration::from_secs(5),
    }
}

#[rustdv::test(timeout_time = 10, timeout_unit = "us")]
async fn fast_recording_is_rejected_without_trace_instrumentation(
    ctx: RustdvCtx,
) -> Result<(), TestError> {
    if std::env::var_os("RUSTDV_VERIFY_FAST_RECORDING_REJECTION").is_none() {
        return Ok(());
    }

    let (mut session, client) = verilator_debug_session_with_config(ctx.dut(), debug_config())
        .map_err(|error| TestError::new(error.to_string()))?;
    let server = McpServer::start(client, server_config())
        .map_err(|error| TestError::new(error.to_string()))?;
    let endpoint = server.endpoint().to_owned();
    let (ready_tx, ready_rx) = mpsc::channel();
    let controller = thread::spawn(move || -> Result<(), String> {
        let runtime = tokio::runtime::Runtime::new()
            .map_err(|error| format!("cannot start controller runtime: {error}"))?;
        runtime.block_on(async move {
            let http = reqwest::Client::new();
            let mut request_id = 0;
            ready_tx
                .send(())
                .map_err(|_| "simulator dropped fast controller".to_owned())?;
            let _: DebugResponse = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "pause_simulation",
                json!({}),
            )
            .await?;
            let error: DebugError = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "start_recording",
                json!({"name": "unavailable"}),
            )
            .await?;
            if !error.message.contains("not built with --trace-fst") {
                return Err(format!("FAST returned the wrong recording error: {error}"));
            }
            let _: DebugResponse = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "terminate_simulation",
                json!({}),
            )
            .await?;
            Ok(())
        })
    });

    ready_rx
        .recv_timeout(Duration::from_secs(1))
        .map_err(|error| TestError::new(format!("fast controller did not start: {error}")))?;
    thread::sleep(Duration::from_millis(200));
    let exit = run_verilator_debug_service(&mut session).await;
    if exit != DebugServiceExit::Terminated {
        return Err(TestError::new(format!(
            "fast debug service exited as {exit:?}"
        )));
    }
    controller
        .join()
        .map_err(|_| TestError::new("fast controller panicked"))?
        .map_err(TestError::new)?;
    drop(server);
    println!("DEBUG CONTROL FAST RECORDING REJECTION: PASS");
    Ok(())
}

#[rustdv::test(timeout_time = 10, timeout_unit = "us")]
async fn fst_recording_control_and_history(ctx: RustdvCtx) -> Result<(), TestError> {
    if std::env::var_os("RUSTDV_VERIFY_FST_RECORDING").is_none() {
        return Ok(());
    }

    let enable = ctx.dut().signal("enable")?;
    enable.set_u64(1);
    read_write().await;

    let trace_config = TraceRecordingConfig {
        max_duration_steps: 8,
        ..TraceRecordingConfig::default()
    };
    let (mut session, client) =
        verilator_debug_session_with_configs(ctx.dut(), debug_config(), trace_config)
            .map_err(|error| TestError::new(error.to_string()))?;
    let server = McpServer::start(client.clone(), server_config())
        .map_err(|error| TestError::new(error.to_string()))?;
    println!("DEBUG MCP: {}", server.endpoint());

    let endpoint = server.endpoint().to_owned();
    let (controller_ready_tx, controller_ready_rx) = mpsc::channel();
    let controller = thread::spawn(move || -> Result<(), String> {
        let runtime = tokio::runtime::Runtime::new()
            .map_err(|error| format!("cannot start controller runtime: {error}"))?;
        runtime.block_on(async move {
            let http = reqwest::Client::new();
            let mut request_id = 0;
            controller_ready_tx
                .send(())
                .map_err(|_| "simulator dropped controller-ready receiver".to_string())?;

            let DebugResponse::Control(initial_pause) = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "pause_simulation",
                json!({}),
            )
            .await?
            else {
                return Err("pause returned the wrong response".into());
            };
            let TraceResponse::Summary(initial_backend) = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "recording_backend_status",
                json!({}),
            )
            .await?
            else {
                return Err("recording backend status returned the wrong response".into());
            };
            if !initial_backend.available
                || initial_backend.recordings != 0
                || initial_backend.max_duration_steps != 8
            {
                return Err(format!(
                    "unexpected initial FST backend: {initial_backend:?}"
                ));
            }

            let invalid_projection: DebugError = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "start_recording",
                json!({"name": "invalid", "signals": ["not_a_signal"], "capacity": 4}),
            )
            .await?;
            if !invalid_projection.message.contains("not present") {
                return Err(format!(
                    "invalid projection returned the wrong error: {invalid_projection:?}"
                ));
            }
            let TraceResponse::Summary(after_invalid) = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "recording_backend_status",
                json!({}),
            )
            .await?
            else {
                return Err("post-error backend status returned the wrong response".into());
            };
            if after_invalid.recordings != 0
                || after_invalid.active_recording.is_some()
                || after_invalid.retained_bytes != 0
            {
                return Err(format!(
                    "failed recording start leaked state or files: {after_invalid:?}"
                ));
            }

            let _: DebugResponse = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "add_watch",
                json!({"name": "done_high", "all_high": ["done"]}),
            )
            .await?;

            // Position the simulation before capture. The trace must contain
            // nothing from this pre-arm interval.
            let positioned_time = initial_pause.simulation_time_steps + 2;
            let _: DebugResponse = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "run_until_time",
                json!({"target_time_steps": positioned_time}),
            )
            .await?;
            let positioned_pause = wait_for_pause(&http, &endpoint, &mut request_id).await?;
            if positioned_pause.simulation_time_steps != positioned_time {
                return Err(format!(
                    "pre-capture positioning failed: {positioned_pause:?}"
                ));
            }
            let DebugResponse::Signal(initial_count) = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "read_signal",
                json!({"path": "count"}),
            )
            .await?
            else {
                return Err("initial count read returned the wrong response".into());
            };

            let TraceResponse::RecordingStatus(started) = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "start_recording",
                json!({"name": "counter", "signals": ["count", "done"], "capacity": 16}),
            )
            .await?
            else {
                return Err("start recording returned the wrong response".into());
            };
            if !started.active
                || started.start_time_steps != positioned_time
                || started.retained != 1
                || started.dropped != 0
                || started.next_cursor != 1
                || started.read_errors != 0
                || started.projection_through_time_steps != Some(positioned_time)
            {
                return Err(format!(
                    "capture did not arm at positioned time: {started:?}"
                ));
            }
            let combined_status: Value = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "simulation_status",
                json!({}),
            )
            .await?;
            if combined_status["value"]["recordings"] != 1
                || combined_status["value"]["trace"]["active_recording"] != "counter"
            {
                return Err(format!(
                    "simulation status omitted FST state: {combined_status}"
                ));
            }

            let _: DebugResponse = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "run_until_time",
                json!({"target_time_steps": positioned_time + 4}),
            )
            .await?;
            let time_pause = wait_for_pause(&http, &endpoint, &mut request_id).await?;
            if time_pause.pause_reason != Some(PauseReason::TargetReached)
                || time_pause.simulation_time_steps != positioned_time + 4
            {
                return Err(format!(
                    "run-until time stopped incorrectly: {time_pause:?}"
                ));
            }

            // The legacy history API remains usable while capture is active.
            // This closes one private FST segment, immediately resumes capture
            // at the same settled time, and returns a change-only projection.
            let TraceResponse::Recording(active_page) = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "get_recording",
                json!({"name": "counter", "limit": 16}),
            )
            .await?
            else {
                return Err("active get_recording returned the wrong response".into());
            };
            if active_page.samples.is_empty()
                || active_page.samples.last().is_none_or(|sample| {
                    sample.simulation_time_steps > time_pause.simulation_time_steps
                })
            {
                return Err(format!(
                    "active recording query returned invalid history: {active_page:?}"
                ));
            }

            let _: DebugResponse = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "run_until_predicate",
                json!({"all_high": ["done"], "all_low": [], "timeout_steps": 20}),
            )
            .await?;
            let predicate_pause = wait_for_pause(&http, &endpoint, &mut request_id).await?;
            if predicate_pause.pause_reason != Some(PauseReason::PredicateMatched) {
                return Err(format!(
                    "run-until predicate stopped incorrectly: {predicate_pause:?}"
                ));
            }

            let TraceResponse::RecordingStatus(stopped) = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "stop_recording",
                json!({"name": "counter"}),
            )
            .await?
            else {
                return Err("stop recording returned the wrong response".into());
            };
            if stopped.active
                || stopped.bytes == 0
                || stopped.signal_count < 5
                || stopped.end_time_steps != predicate_pause.simulation_time_steps
                || stopped.retained < 3
                || stopped.projection_through_time_steps != Some(stopped.end_time_steps)
            {
                return Err(format!("stopped capture status is incomplete: {stopped:?}"));
            }

            let TraceResponse::Hierarchy(hierarchy) = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "recording_hierarchy",
                json!({"name": "counter", "max_depth": 8, "max_results": 64}),
            )
            .await?
            else {
                return Err("recording hierarchy returned the wrong response".into());
            };
            let hidden_path = hierarchy
                .iter()
                .find(|entry| entry.path.ends_with(".hidden_state"))
                .map(|entry| entry.path.clone())
                .ok_or_else(|| format!("internal hidden_state missing from FST: {hierarchy:?}"))?;

            let TraceResponse::ValueAt(hidden_at_stop) = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "recording_value_at",
                json!({
                    "name": "counter",
                    "signal": hidden_path,
                    "time_steps": stopped.end_time_steps
                }),
            )
            .await?
            else {
                return Err("recording value query returned the wrong response".into());
            };
            if hidden_at_stop.sampled_time_steps < started.start_time_steps {
                return Err(format!(
                    "trace returned a pre-arm value: {hidden_at_stop:?}"
                ));
            }

            let TraceResponse::Changes(hidden_changes) = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "recording_changes",
                json!({
                    "name": "counter",
                    "signal": hidden_at_stop.value.path,
                    "start_time_steps": started.start_time_steps,
                    "end_time_steps": stopped.end_time_steps,
                    "limit": 32
                }),
            )
            .await?
            else {
                return Err("recording changes returned the wrong response".into());
            };
            if hidden_changes.changes.len() < 3
                || hidden_changes.changes.iter().any(|change| {
                    change.simulation_time_steps < started.start_time_steps
                        || change.simulation_time_steps > stopped.end_time_steps
                })
            {
                return Err(format!("hidden history is incomplete: {hidden_changes:?}"));
            }

            let TraceResponse::Snapshot(snapshot) = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "recording_snapshot",
                json!({
                    "name": "counter",
                    "scope": "debug_control_probe",
                    "time_steps": stopped.end_time_steps,
                    "max_signals": 32
                }),
            )
            .await?
            else {
                return Err("recording snapshot returned the wrong response".into());
            };
            if !snapshot
                .values
                .iter()
                .any(|value| value.path.ends_with(".hidden_state"))
            {
                return Err(format!("snapshot omitted hidden_state: {snapshot:?}"));
            }

            let TraceResponse::Recording(RecordingPage { samples, .. }) = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "get_recording",
                json!({"name": "counter", "limit": 16}),
            )
            .await?
            else {
                return Err("get_recording returned the wrong response".into());
            };
            if samples.len() < 3
                || !samples.windows(2).all(|pair| {
                    pair[0].cursor < pair[1].cursor
                        && pair[0].simulation_time_steps < pair[1].simulation_time_steps
                        && pair[0].values != pair[1].values
                })
            {
                return Err(format!("legacy projection is not change-only: {samples:?}"));
            }
            if samples[0].values[0] != initial_count {
                return Err("recording initial projection does not match arm-time count".into());
            }

            let DebugResponse::Signal(current_count) = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "read_signal",
                json!({"path": "count"}),
            )
            .await?
            else {
                return Err("current count read returned the wrong response".into());
            };
            if samples.last().unwrap().values[0] != current_count {
                return Err("recording tail does not match the stop-time count".into());
            }

            // Advance after stop, then prove history cannot extend beyond the
            // closed capture window.
            let _: DebugResponse = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "run_until_predicate",
                json!({"all_high": [], "all_low": ["done"], "timeout_steps": 2}),
            )
            .await?;
            let timeout_pause = wait_for_pause(&http, &endpoint, &mut request_id).await?;
            if timeout_pause.pause_reason != Some(PauseReason::PredicateTimeout) {
                return Err(format!(
                    "predicate timeout stopped incorrectly: {timeout_pause:?}"
                ));
            }
            let after_stop_error: DebugError = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "recording_value_at",
                json!({
                    "name": "counter",
                    "signal": hidden_at_stop.value.path,
                    "time_steps": timeout_pause.simulation_time_steps
                }),
            )
            .await?;
            if !after_stop_error.message.contains("outside capture window") {
                return Err(format!(
                    "post-stop history query was not rejected: {after_stop_error}"
                ));
            }

            // A second recording deliberately reaches its configured duration
            // bound; status must expose the automatic stop and its reason.
            let TraceResponse::RecordingStatus(limited_start) = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "start_recording",
                json!({"name": "limited", "capacity": 4}),
            )
            .await?
            else {
                return Err("limited recording did not start".into());
            };
            let control_target = limited_start.start_time_steps + 32;
            let _: DebugResponse = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "run_until_time",
                json!({"target_time_steps": control_target}),
            )
            .await?;
            let limit_pause = wait_for_pause(&http, &endpoint, &mut request_id).await?;
            if limit_pause.simulation_time_steps != control_target {
                return Err(format!(
                    "sparse-event control did not pause at its independent deadline: {limit_pause:?}"
                ));
            }
            let TraceResponse::RecordingStatus(limited_status) = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "recording_status",
                json!({"name": "limited"}),
            )
            .await?
            else {
                return Err("limited recording status was the wrong response".into());
            };
            if limited_status.active
                || !limited_status.limit_reached
                || limited_status.end_time_steps != limited_start.start_time_steps + 8
                || !limited_status
                    .limit_reason
                    .as_deref()
                    .is_some_and(|reason| reason.contains("duration"))
            {
                return Err(format!(
                    "duration limit was not visible: {limited_status:?}"
                ));
            }

            let _: TraceResponse = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "remove_recording",
                json!({"name": "counter"}),
            )
            .await?;
            let _: TraceResponse = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "remove_recording",
                json!({"name": "limited"}),
            )
            .await?;
            let TraceResponse::Summary(final_backend) = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "recording_backend_status",
                json!({}),
            )
            .await?
            else {
                return Err("final backend status returned the wrong response".into());
            };
            if final_backend.recordings != 0 || final_backend.retained_bytes != 0 {
                return Err(format!(
                    "removed recordings retained resources: {final_backend:?}"
                ));
            }

            let _: DebugResponse = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "terminate_simulation",
                json!({}),
            )
            .await?;
            Ok(())
        })
    });

    controller_ready_rx
        .recv_timeout(Duration::from_secs(1))
        .map_err(|error| TestError::new(format!("controller did not start: {error}")))?;
    thread::sleep(Duration::from_millis(500));

    let exit = run_verilator_debug_service(&mut session).await;
    if exit != DebugServiceExit::Terminated {
        return Err(TestError::new(format!("debug service exited as {exit:?}")));
    }
    controller
        .join()
        .map_err(|_| TestError::new("debug controller panicked"))?
        .map_err(TestError::new)?;
    drop(server);
    println!("DEBUG FST CONTROL HISTORY: PASS");
    Ok(())
}

// The same Rust testbench and MCP commands exercise the VPI debug front end
// on both Icarus and Verilator. Only the trace backend is simulator-specific.
#[rustdv::test(timeout_time = 10, timeout_unit = "us")]
async fn portable_debug_frontend_runs_on_both_simulators(ctx: RustdvCtx) -> Result<(), TestError> {
    if std::env::var_os("RUSTDV_VERIFY_PORTABLE_DEBUG").is_none() {
        return Ok(());
    }

    let (mut session, client) = simulator_debug_session_with_config(ctx.dut(), debug_config())
        .map_err(|error| TestError::new(error.to_string()))?;
    let server = McpServer::start(client, server_config())
        .map_err(|error| TestError::new(error.to_string()))?;
    let endpoint = server.endpoint().to_owned();
    let (ready_tx, ready_rx) = mpsc::channel();
    let controller = thread::spawn(move || -> Result<(), String> {
        let runtime = tokio::runtime::Runtime::new()
            .map_err(|error| format!("cannot start controller runtime: {error}"))?;
        runtime.block_on(async move {
            let http = reqwest::Client::new();
            let mut request_id = 0;
            ready_tx
                .send(())
                .map_err(|_| "simulator dropped portable controller".to_owned())?;

            let DebugResponse::Control(paused) = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "pause_simulation",
                json!({}),
            )
            .await?
            else {
                return Err("pause returned the wrong response".into());
            };
            let status: Value = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "simulation_status",
                json!({}),
            )
            .await?;
            let trace = status["value"].get("trace");
            let expected_trace = std::env::var_os("RUSTDV_EXPECT_TRACE_BACKEND").as_deref()
                == Some(std::ffi::OsStr::new("1"));
            let backend_matches = if expected_trace {
                trace
                    .and_then(|value| value.get("backend"))
                    .and_then(Value::as_str)
                    == Some("fst")
            } else {
                trace.is_none()
            };
            if !backend_matches {
                let _ = call_mcp_tool::<DebugResponse>(
                    &http,
                    &endpoint,
                    &mut request_id,
                    "terminate_simulation",
                    json!({}),
                )
                .await;
                return Err(format!("unexpected trace backend selection: {status}"));
            }
            let DebugResponse::Signal(signal) = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "read_signal",
                json!({"path": "clk"}),
            )
            .await?
            else {
                return Err("live signal read returned the wrong response".into());
            };
            if signal.width != 1 {
                return Err(format!("clk had width {}, not 1", signal.width));
            }

            let _: DebugResponse = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "start_recording",
                json!({"name": "portable", "signals": ["clk"], "capacity": 16}),
            )
            .await?;
            let _: DebugResponse = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "run_until_time",
                json!({"target_time_steps": paused.simulation_time_steps + 3}),
            )
            .await?;
            wait_for_pause(&http, &endpoint, &mut request_id).await?;
            let DebugResponse::Recording(page) = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "get_recording",
                json!({"name": "portable", "limit": 16}),
            )
            .await?
            else {
                return Err("recording query returned the wrong response".into());
            };
            if page.samples.len() < 2 {
                let _ = call_mcp_tool::<DebugResponse>(
                    &http,
                    &endpoint,
                    &mut request_id,
                    "terminate_simulation",
                    json!({}),
                )
                .await;
                return Err(format!("recording missed clock changes: {page:?}"));
            }
            let _: DebugResponse = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "stop_recording",
                json!({"name": "portable"}),
            )
            .await?;
            let _: DebugResponse = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "terminate_simulation",
                json!({}),
            )
            .await?;
            Ok(())
        })
    });

    ready_rx
        .recv_timeout(Duration::from_secs(1))
        .map_err(|error| TestError::new(format!("controller did not start: {error}")))?;
    thread::sleep(Duration::from_millis(100));
    let exit = run_debug_service(&mut session).await;
    if exit != DebugServiceExit::Terminated {
        return Err(TestError::new(format!("debug service exited as {exit:?}")));
    }
    controller
        .join()
        .map_err(|_| TestError::new("portable debug controller panicked"))?
        .map_err(TestError::new)?;
    drop(server);
    println!("PORTABLE DEBUG FRONTEND: PASS");
    Ok(())
}
