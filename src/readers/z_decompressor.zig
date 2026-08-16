const std = @import("std");
const Allocator = std.mem.Allocator;

const c = @import("c.zig").c;

const InflateResult = struct {
    consumed_bytes: u32,
    produced_bytes: u32,
    input_finished: bool,
    stream_finished: bool,
};

pub const Zdecompressor = struct {
    stream: c.z_stream,
    initialized: bool,
    finished: bool,

    pub fn create(allocator: Allocator) !*Zdecompressor {
        const self = try allocator.create(Zdecompressor);
        errdefer allocator.destroy(self);

        self.* = .{
            .stream = std.mem.zeroes(c.z_stream),
            .initialized = false,
            .finished = false,
        };

        const status = c.inflateInit2(
            &self.stream,
            16 + c.MAX_WBITS,
        );

        if (status != c.Z_OK) {
            std.log.err(
                \\ [Zlib]: initilisation failed...
                \\  Error: {s}
            ,
                .{std.mem.span(c.zError(status))},
            );
            return error.ZlibInitializationFailed;
        }

        self.initialized = true;

        std.debug.assert(self.stream.avail_in == 0);
        std.debug.assert(self.stream.avail_out == 0);
        std.debug.assert(!self.finished);

        return self;
    }

    pub fn destroy(
        self: *Zdecompressor,
        allocator: Allocator,
    ) void {
        std.debug.assert(self.initialized);

        const status = c.inflateEnd(&self.stream);

        std.debug.assert(
            status == c.Z_OK or
                status == c.Z_DATA_ERROR,
        );

        self.initialized = false;
        allocator.destroy(self);
    }

    pub fn setInput(
        self: *Zdecompressor,
        input: []const u8,
    ) !void {
        std.debug.assert(self.initialized);
        std.debug.assert(!self.finished);
        std.debug.assert(!self.hasPendingInput());
        std.debug.assert(input.len > 0);

        if (input.len > std.math.maxInt(c_uint)) {
            return error.ZlibInputTooLarge;
        }

        self.stream.next_in = @ptrCast(
            @constCast(input.ptr),
        );
        self.stream.avail_in = @intCast(input.len);

        std.debug.assert(self.stream.next_in != null);
        std.debug.assert(self.stream.avail_in == input.len);
    }

    pub fn inflateInto(
        self: *Zdecompressor,
        output: []u8,
    ) !InflateResult {
        std.debug.assert(self.initialized);
        std.debug.assert(!self.finished);
        std.debug.assert(self.hasPendingInput());
        std.debug.assert(output.len > 0);

        if (output.len > std.math.maxInt(c_uint)) {
            return error.ZlibOutputTooLarge;
        }

        const input_before: c_uint = self.stream.avail_in;

        self.stream.next_out = output.ptr;
        self.stream.avail_out = @intCast(output.len);

        const output_before: c_uint = self.stream.avail_out;

        const status = c.inflate(
            &self.stream,
            c.Z_NO_FLUSH,
        );

        const consumed_c: c_uint = input_before - self.stream.avail_in;
        const produced_c: c_uint = output_before - self.stream.avail_out;

        const consumed_bytes: u32 = @intCast(consumed_c);
        const produced_bytes: u32 = @intCast(produced_c);

        switch (status) {
            c.Z_OK => {},
            c.Z_STREAM_END => {
                self.finished = true;
            },
            c.Z_BUF_ERROR => {
                if (consumed_bytes == 0 and produced_bytes == 0) {
                    return error.ZlibNoProgress;
                }
            },
            c.Z_NEED_DICT => {
                return error.ZlibDictinaryRequired;
            },
            c.Z_DATA_ERROR => {
                return error.ZlibInvalidCompressedData;
            },
            c.Z_MEM_ERROR => {
                return error.ZlibOutOfMemory;
            },
            else => {
                std.log.err(
                    "inflate failed with status {d}",
                    .{status},
                );
                return error.ZlibDecompressionFailed;
            },
        }

        if (self.stream.avail_in == 0) {
            self.stream.next_in = null;
        }

        std.debug.assert(
            consumed_bytes > 0 or
                produced_bytes > 0,
        );
        std.debug.assert(
            produced_bytes <= output.len,
        );

        return .{
            .consumed_bytes = consumed_bytes,
            .produced_bytes = produced_bytes,
            .input_finished = self.stream.avail_in == 0,
            .stream_finished = self.finished,
        };
    }

    pub inline fn hasPendingInput(
        self: *Zdecompressor
    ) bool {
        return self.stream.avail_in > 0;
    }
};
