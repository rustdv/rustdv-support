use rustdv::prelude::*;
use rustdv_mcp::{
    run_debug_service, simulator_debug_session_with_config, DebugServiceExit,
    DebugSessionConfig, McpServer, McpServerConfig,
};
use std::time::Duration;

#[cfg(test)]
use rustdv_vpi_stubs as _;

rustdv::vpi_bootstrap!();

#[rustdv::test]
async fn interactive_debug(ctx: RustdvCtx) -> Result<(), TestError> {
    // Keep the running simulator reaching stable points while the MCP client
    // is not holding it paused.
    let clk = ctx.dut().signal("clk")?;
    Clock::new(&clk, SimDuration::ns(2)).start();

    let debug_config = DebugSessionConfig {
        pause_inactivity_timeout: Duration::from_secs(30),
        ..DebugSessionConfig::default()
    };
    let (mut session, client) = simulator_debug_session_with_config(ctx.dut(), debug_config)
    .map_err(|error| TestError::new(error.to_string()))?;

    let server = McpServer::start(
        client,
        McpServerConfig {
            bind: "127.0.0.1:0".parse().unwrap(),
            request_timeout: Duration::from_secs(5),
        },
    )
    .map_err(|error| TestError::new(error.to_string()))?;
    println!("RustDV MCP endpoint: {}", server.endpoint());

    match run_debug_service(&mut session).await {
        DebugServiceExit::Terminated => println!("debug session terminated"),
        DebugServiceExit::ControllerDisconnected => {
            println!("debug controller disconnected; simulation released")
        }
    }

    drop(server);
    Ok(())
}
