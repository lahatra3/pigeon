const std = @import("std");
const linux = std.os.linux;
const Io = std.Io;
const Allocator = std.mem.Allocator;

const pg_module = @import("writers/pg_copy.zig");
const PgCopyIn = pg_module.PgCopyIn;
const PutDataResult = pg_module.PutDataResult;

const buffer_module = @import("utils/buffer_pool.zig");
const BufferPool = buffer_module.BufferPool;
const COMPRESSED_SLOT_COUNT = buffer_module.COMPRESSED_SLOT_COUNT;
const DECOMPRESSED_SLOT_COUNT = buffer_module.DECOMPRESSED_SLOT_COUNT;

const Zdecompressor = @import("readers/z_decompressor.zig").Zdecompressor;

const drive_budget_max: u16 = 64;

const Tag = enum(u64) {
    disk_read_0 = 10,
    disk_read_1 = 11,
    socket_poll_out = 20,

    pub inline fn toU64(self: Tag) u64 {
        return @intFromEnum(self);
    }
};

const Metrics = struct {
    compressed_bytes: u64 = 0,
    decompressed_bytes: u64 = 0,

    disk_read_count: u64 = 0,
    inflate_count: u64 = 0,
    copy_attempt_count: u64 = 0,
    copy_would_block_count: u64 = 0,
    poll_out_count: u64 = 0,
};

pub const Handler = struct {
    io: Io,
    allocator: Allocator,

    ring: linux.IoUring,
    file: Io.File,
    pg: PgCopyIn,

    decompressor: *Zdecompressor,
    buffers: BufferPool,

    next_file_offset_bytes: u64 = 0,

    read_in_flight: bool = false,
    poll_out_in_flight: bool = false,

    physical_eof_reached: bool = false,
    gzip_stream_finished: bool = false,

    active_compressed_slot: ?u8 = null,
    pending_copy_slot: ?u8 = null,

    metris: Metrics = .{},

    pub fn init(
        io: Io,
        allocator: Allocator,
        file_path: []const u8,
        conn_info: [:0]const u8,
        queue_depth: u16,
    ) !Handler {
        std.debug.assert(file_path.len > 0);
        std.debug.assert(conn_info.len > 0);

        std.debug.assert(queue_depth >= 4);
        std.debug.assert(queue_depth <= 64);

        var ring = try linux.IoUring.init(
            queue_depth,
            0,
        );
        errdefer ring.deinit();

        const file = try std.Io.Dir.openFileAbsolute(
            io,
            file_path,
            .{ .mode = .read_only },
        );
        errdefer file.close(io);

        var pg = try PgCopyIn.init(conn_info);
        errdefer pg.deinit();

        const decompressor = try Zdecompressor.create(allocator);
        errdefer decompressor.destroy(allocator);

        var buffers = try BufferPool.init(allocator);
        errdefer buffers.deinit();

        const handler = Handler{
            .io = io,
            .allocator = allocator,
            .ring = ring,
            .file = file,
            .pg = pg,
            .decompressor = decompressor,
            .buffers = buffers,
        };

        handler.assertInvariants();

        return handler;
    }

    pub fn deinit(self: *Handler) void {
        std.debug.assert(!self.read_in_flight);
        std.debug.assert(!self.poll_out_in_flight);
        std.debug.assert(self.pending_copy_slot == null);
        std.debug.assert(self.active_compressed_slot == null);

        self.buffers.deinit();
        self.decompressor.destroy(self.allocator);
        self.pg.deinit();
        self.file.close(self.io);
        self.ring.deinit();
    }

    pub fn run(
        self: *Handler,
        copy_sql: [:0]const u8,
    ) !void {
        std.debug.assert(copy_sql.len > 0);

        try self.pg.startCopy(copy_sql);

        try self.maybeSubmitRead();

        while (!self.processingFinished()) {
            try self.drivePipeline();

            if (self.processingFinished()) break;

            if (!self.read_in_flight and !self.poll_out_in_flight) {
                return error.PipelineDeadlock;
            }

            _ = try self.ring.submit_and_wait(1);

            try self.processCompletions();
        }

        std.debug.assert(self.pending_copy_slot == null);
        std.debug.assert(self.active_compressed_slot == null);
        std.debug.assert(!self.read_in_flight);

        while (!try self.pg.flush()) {
            if (!self.poll_out_in_flight) {
                try self.submitPollOut();
            }

            _ = try self.ring.submit_and_wait(1);
            try self.processCompletions();
        }

        std.debug.assert(!self.poll_out_in_flight);

        try self.pg.finalizeCopy();

        self.logMetrics();
    }

    fn drivePipeline(self: *Handler) !void {
        var operation_count: u16 = 0;

        while (operation_count < drive_budget_max) : (operation_count += 1) {
            self.assertInvariants();

            const progressed = try self.driveOne();

            self.assertInvariants();

            if (!progressed) return;
        }
    }

    fn driveOne(self: *Handler) !bool {
        if (self.pending_copy_slot) |slot_index| {
            return try self.trySendPending(slot_index);
        }

        if (self.gzip_stream_finished) {
            self.releaseActiveCompressedIfConsumed();
            return false;
        }

        if (self.active_compressed_slot == null) {
            const ready_slot = self.buffers.findReadyCompressed() orelse {
                try self.maybeSubmitRead();
                return false;
            };

            try self.activateCompressed(ready_slot);
        }

        const output_slot = self.buffers.findFreeDecompressed() orelse {
            return false;
        };

        try self.inflateOne(output_slot);
        return true;
    }

    fn activateCompressed(
        self: *Handler,
        slot_index: u8,
    ) !void {
        std.debug.assert(slot_index < COMPRESSED_SLOT_COUNT);

        std.debug.assert(self.active_compressed_slot == null);

        std.debug.assert(!self.decompressor.hasPendingInput());

        const slot = self.buffers.compressedSlot(slot_index);

        std.debug.assert(slot.state == .ready);
        std.debug.assert(slot.length_bytes > 0);

        slot.state = .inflating;
        self.active_compressed_slot = slot_index;

        const length: usize = @intCast(slot.length_bytes);

        try self.decompressor.setInput(slot.buffer[0..length]);

        try self.maybeSubmitRead();
    }

    fn inflateOne(
        self: *Handler,
        output_slot_index: u8,
    ) !void {
        std.debug.assert(output_slot_index < DECOMPRESSED_SLOT_COUNT);

        std.debug.assert(self.active_compressed_slot != null);

        std.debug.assert(self.pending_copy_slot == null);
    }

    fn trySendPending(self: *Handler) !bool {
        _ = self;
    }

    fn releaseActiveCompressedIfConsumed(self: *Handler) void {
        _ = self;
    }

    fn processCompletions(self: *Handler) !void {
        _ = self;
    }

    fn submitPollOut(self: *Handler) !void {
        _ = self;
    }

    fn maybeSubmitRead(self: *Handler) !void {
        _ = self;
    }

    fn processingFinished(self: *Handler) !void {
        _ = self;
    }

    fn logMetrics(self: *Handler) void {
        _ = self;
    }

    fn assertInvariants(self: *Handler) void {
        _ = self;
    }
};
