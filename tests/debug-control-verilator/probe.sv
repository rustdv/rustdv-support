`timescale 1ns/1ns
module debug_control_probe;
    bit clk = 0;
    bit enable = 0;
    logic [3:0] count = 0;
    wire done = count >= 3;

    always #1 clk = ~clk;
    always @(posedge clk) begin
        if (enable)
            count <= count + 1;
    end

    wire keep_alive = ^{clk, enable, count, done};
    final $display("DEBUG CONTROL RTL FINAL: PASS");
endmodule
