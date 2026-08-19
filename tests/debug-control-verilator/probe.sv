`timescale 1ns/1ns
module debug_control_probe(
    input  bit         enable,
    output logic [3:0] count,
    output wire        done
);
    bit clk = 0;
    logic [7:0] hidden_state = 8'h41;
    initial count = 0;
    assign done = count >= 5;

    // Stop producing RTL events once the first scenario reaches `done`.
    // The recording-duration regression must then be woken only by the MCP
    // service's trace deadline, not by a convenient free-running clock.
    initial begin
        while (!done) begin
            #1 clk = ~clk;
        end
    end
    always @(posedge clk) begin
        if (enable) begin
            count <= count + 1;
            hidden_state <= hidden_state + 3;
        end
    end

    wire keep_alive = ^{clk, enable, count, done, hidden_state};
    final $display("DEBUG CONTROL RTL FINAL: PASS");
endmodule
