use rustdv::prelude::*;
use rustdv_debug::{ControlMode, DebugResponse, PauseReason, RecordingPage};
use rustdv_mcp_verilator::{
    run_verilator_debug_service, verilator_debug_session_with_config, DebugServiceExit,
    DebugSessionConfig, McpServer, McpServerConfig,
};
use serde_json::{json, Value};
use std::{sync::mpsc, thread, time::Duration};

#[cfg(test)]
use rustdv_vpi_stubs as _;

rustdv::vpi_bootstrap!();

async fn call_mcp_tool(
    http: &reqwest::Client,
    endpoint: &str,
    request_id: &mut u64,
    name: &str,
    arguments: Value,
) -> Result<DebugResponse, String> {
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
        format!("MCP tool {name} returned invalid debug response: {error}: {text}")
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

#[rustdv::test(timeout_time = 10, timeout_unit = "us")]
async fn debug_control_history_inspect(ctx: RustdvCtx) -> Result<(), TestError> {
    let enable = ctx.dut().signal("enable")?;
    enable.set_u64(1);
    read_write().await;

    let (mut session, client) = verilator_debug_session_with_config(
        ctx.dut(),
        DebugSessionConfig {
            pause_inactivity_timeout: Duration::from_secs(2),
            ..DebugSessionConfig::default()
        },
    )
    .map_err(|error| TestError::new(error.to_string()))?;
    let server = McpServer::start(
        client.clone(),
        McpServerConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            request_timeout: Duration::from_secs(2),
        },
    )
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
            println!("DEBUG MCP CONTROLLER: sending pause");
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
            println!("DEBUG MCP CONTROLLER: pause acknowledged");
            let DebugResponse::Status(first_status) = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "simulation_status",
                json!({}),
            )
            .await?
            else {
                return Err("status returned the wrong response".into());
            };
            let DebugResponse::Signal(initial_count) = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "read_signal",
                json!({"path": "count"}),
            )
            .await?
            else {
                return Err("read_signal returned the wrong response".into());
            };
            let DebugResponse::Status(second_status) = call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "simulation_status",
                json!({}),
            )
            .await?
            else {
                return Err("second status returned the wrong response".into());
            };
            if initial_pause.simulation_time_steps != first_status.simulation_time_steps
                || first_status.simulation_time_steps != second_status.simulation_time_steps
            {
                return Err("simulation time changed while paused requests were serviced".into());
            }

            call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "start_recording",
                json!({"name": "counter", "signals": ["count", "done"], "capacity": 16}),
            )
            .await?;
            call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "run_until_time",
                json!({"target_time_steps": first_status.simulation_time_steps + 4}),
            )
            .await?;
            let time_pause = wait_for_pause(&http, &endpoint, &mut request_id).await?;
            if time_pause.pause_reason != Some(PauseReason::TargetReached)
                || time_pause.simulation_time_steps != first_status.simulation_time_steps + 4
            {
                return Err(format!(
                    "run-until time stopped incorrectly: {time_pause:?}"
                ));
            }

            call_mcp_tool(
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

            let DebugResponse::Recording(RecordingPage { samples, .. }) = call_mcp_tool(
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
            if samples.len() < 2 {
                return Err(format!(
                    "recording captured only {} sample(s)",
                    samples.len()
                ));
            }
            if !samples.windows(2).all(|pair| {
                pair[0].cursor < pair[1].cursor
                    && pair[0].simulation_time_steps < pair[1].simulation_time_steps
                    && pair[0].values != pair[1].values
            }) {
                return Err("recording history is not ordered change-only data".into());
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
                return Err("recording tail does not match the current count".into());
            }
            if samples[0].values[0] != initial_count {
                return Err("recording initial snapshot does not match the paused count".into());
            }

            call_mcp_tool(
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

            call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "resume_simulation",
                json!({}),
            )
            .await?;
            call_mcp_tool(
                &http,
                &endpoint,
                &mut request_id,
                "pause_simulation",
                json!({}),
            )
            .await?;
            let resumed_pause = wait_for_pause(&http, &endpoint, &mut request_id).await?;
            if resumed_pause.simulation_time_steps <= timeout_pause.simulation_time_steps {
                return Err(format!(
                    "simulation did not advance after resume: before={timeout_pause:?}, after={resumed_pause:?}"
                ));
            }

            call_mcp_tool(
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
    // Give the real HTTP handler a bounded wall-clock window to enqueue the
    // initial pause before this deliberately tiny DUT can finish in sim time.
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
    println!("DEBUG CONTROL HISTORY: PASS");
    Ok(())
}
