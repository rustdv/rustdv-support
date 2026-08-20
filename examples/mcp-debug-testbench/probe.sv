`timescale 1ns/1ns

module debug_probe (
    input  logic       clk,
    output logic [7:0] count,
    output logic       done,
    output wire        internal_activity
);
    logic [7:0] hidden_state;
    logic [3:0] selected_debug_state;

    initial begin
        count = 0;
        done = 0;
        hidden_state = 8'h41;
        selected_debug_state = 0;
    end

    always @(posedge clk) begin
        count <= count + 1'b1;
        hidden_state <= hidden_state + 8'h03;
        selected_debug_state <= count[3:0] ^ hidden_state[3:0];
        done <= count >= 8'd20;
    end

    // Give the internal state an observable cone so the example does not rely
    // on trace instrumentation alone to prevent it being optimized away.
    assign internal_activity = ^{hidden_state, selected_debug_state};

    final $display("MCP DEBUG EXAMPLE RTL FINAL");
endmodule
