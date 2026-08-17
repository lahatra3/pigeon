const std = @import("std");
const linux = std.os.linux;
const posix = std.posix;
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

        const output_slot = self.buffers.decompressedSlot(output_slot_index);

        std.debug.assert(output_slot.state == .free);
        std.debug.assert(output_slot.length_bytes == 0);

        output_slot.state = .filling;

        const res = self.decompressor.inflateInto(output_slot.buffer);

        const inflate_res = res catch |err| {
            self.buffers.resetDecompressedFill(output_slot_index);

            return err;
        };

        self.metris.inflate_count += 1;

        if (inflate_res.produced_bytes > 0) {
            output_slot.length_bytes = inflate_res.produced_bytes;

            output_slot.state = .pending_copy;
            self.pending_copy_slot = output_slot_index;

            self.metris.decompressed_bytes += inflate_res.produced_bytes;
        } else {
            self.buffers.resetDecompressedFill(output_slot_index);
        }

        if (inflate_res.input_finished) {
            self.releaseActiveCompressedIfConsumed();
            try self.maybeSubmitRead();
        }

        if (inflate_res.stream_finished) {
            self.gzip_stream_finished = true;
            self.releaseActiveCompressedIfConsumed();
        }
    }

    fn trySendPending(
        self: *Handler,
        slot_index: u8,
    ) !bool {
        std.debug.assert(slot_index < DECOMPRESSED_SLOT_COUNT);

        std.debug.assert(self.pending_copy_slot == slot_index);

        const slot = self.buffers.decompressedSlot(slot_index);

        std.debug.assert(slot.state == .pending_copy);

        std.debug.assert(slot.length_bytes > 0);

        const length: usize = @intCast(slot.length_bytes);

        const res: PutDataResult = try self.pg.putData(slot.buffer[0..length]);

        self.metris.copy_attempt_count += 1;

        switch (res) {
            .accepted => {
                self.pending_copy_slot = null;
                self.buffers.releaseDecompressed(slot_index);

                const fully_flushed = try self.pg.flush();

                if (!fully_flushed and !self.poll_out_in_flight) {
                    try self.submitPollOut();
                }

                return true;
            },
            .would_block => {
                self.metris.copy_would_block_count += 1;

                const fully_flushed = try self.pg.flush();

                if (!fully_flushed) {
                    try self.submitPollOut();
                } else {
                    try self.submitPollOut();
                }

                return false;
            },
        }
    }

    fn maybeSubmitRead(self: *Handler) !void {
        if (self.read_in_flight) return;
        if (self.physical_eof_reached) return;
        if (self.gzip_stream_finished) return;

        const slot_index = self.buffers.findFreeCompressed() orelse {
            return;
        };

        const slot = self.buffers.compressedSlot(slot_index);

        std.debug.assert(slot.state == .free);
        std.debug.assert(slot.length_bytes == 0);

        slot.state = .reading;
        slot.file_offset_bytes = self.next_file_offset_bytes;

        errdefer {
            slot.state = .free;
            slot.file_offset_bytes = 0;
        }

        const tag: Tag = switch (slot_index) {
            0 => .disk_read_0,
            1 => .disk_read_1,
            else => unreachable,
        };

        const sqe = try self.ring.get_sqe();

        sqe.prep_read(
            self.file.handle,
            slot.buffer,
            slot.file_offset_bytes,
        );

        sqe.user_data = tag.toU64();

        self.next_file_offset_bytes += slot.buffer.len;

        self.read_in_flight = true;
    }

    fn submitPollOut(self: *Handler) !void {
        if (self.poll_out_in_flight) return;

        const sqe = try self.ring.get_sqe();

        sqe.prep_poll_add(
            self.pg.socket,
            linux.POLL.OUT,
        );

        sqe.user_data = Tag.socket_poll_out.toU64();

        self.poll_out_in_flight = true;
        self.metris.poll_out_count += 1;
    }

    fn processCompletions(self: *Handler) !void {
        var completion_count: u16 = 0;
        const completion_limit: u16 = 128;

        while (self.ring.cq_ready() > 0 and
            completion_count < completion_limit) : (completion_count += 1)
        {
            const completion = try self.ring.copy_cqe();

            try self.handleCompletion(completion);
        }
    }

    fn handleCompletion(
        self: *Handler,
        completion: linux.io_uring_cqe,
    ) !void {
        if (completion.res < 0) {
            const error_number = -completion.res;

            const system_error: posix.E = @enumFromInt(error_number);

            std.log.err(
                \\ [io_uring]: operation failed...
                \\ Tag: {d}
                \\ Error: {s}
                \\ Code: {d}
            ,
                .{
                    completion.user_data,
                    @tagName(system_error),
                    error_number,
                },
            );

            return error.IoUringOperationFailed;
        }

        const tag: Tag = @enumFromInt(completion.user_data);

        switch (tag) {
            .disk_read_0, .disk_read_1 => {
                try self.handleDiskRead(
                    completion,
                    tag,
                );
            },
            .socket_poll_out => {
                try self.handlePollout();
            },
        }
    }

    fn handleDiskRead(
        self: *Handler,
        completion: linux.io_uring_cqe,
        tag: Tag,
    ) !void {
        std.debug.assert(self.read_in_flight);

        self.read_in_flight = false;
        self.metris.disk_read_count += 1;

        const slot_index: u8 = switch (tag) {
            .disk_read_0 => 0,
            .disk_read_1 => 1,
            else => unreachable,
        };

        const slot = self.buffers.compressedSlot(slot_index);

        std.debug.assert(slot.state == .reading);

        const bytes_read: u32 = @intCast(completion.res);

        if (bytes_read == 0) {
            self.physical_eof_reached = true;

            self.buffers.resetCompressedRead(slot_index);

            return;
        }

        const reserved_end = slot.file_offset_bytes + slot.buffer.len;

        std.debug.assert(self.next_file_offset_bytes >= reserved_end);

        self.next_file_offset_bytes = slot.file_offset_bytes + bytes_read;

        self.metris.compressed_bytes += bytes_read;

        if (self.gzip_stream_finished) {
            self.buffers.resetCompressedRead(slot_index);

            return;
        }

        slot.length_bytes = bytes_read;
        slot.state = .ready;

        try self.maybeSubmitRead();
    }

    fn handlePollout(self: *Handler) !void {
        std.debug.assert(self.poll_out_in_flight);

        self.poll_out_in_flight = false;

        const fully_flushed = try self.pg.flush();

        if (!fully_flushed) {
            try self.submitPollOut();
            return;
        }

        try self.pg.consumeAvailableInput();
    }

    fn releaseActiveCompressedIfConsumed(self: *Handler) void {
        if (self.active_compressed_slot) |slot_index| {
            if (self.decompressor.hasPendingInput()) {
                return;
            }

            self.buffers.releaseCompressed(slot_index);
            self.active_compressed_slot = null;
        }
    }

    fn processingFinished(self: *const Handler) bool {
        if (!self.gzip_stream_finished) return false;
        if (self.pending_copy_slot != null) return false;
        if (self.active_compressed_slot != null) return false;

        if (self.read_in_flight) return false;

        return true;
    }

    fn assertInvariants(self: *const Handler) void {
        self.buffers.assertInvariants();

        if (self.active_compressed_slot) |slot_index| {
            std.debug.assert(slot_index < COMPRESSED_SLOT_COUNT);

            const slot = self.buffers.compressed[slot_index];
            std.debug.assert(
                slot.state == .inflating,
            );
        }

        if (self.pending_copy_slot) |slot_index| {
            std.debug.assert(slot_index < DECOMPRESSED_SLOT_COUNT);

            const slot = self.buffers.decompressed[slot_index];
            std.debug.assert(slot.state == .pending_copy);

            std.debug.assert(slot.length_bytes > 0);
        }

        var reading_count: u8 = 0;

        for (self.buffers.compressed) |slot| {
            if (slot.state == .reading) {
                reading_count += 1;
            }
        }

        if (self.read_in_flight) {
            std.debug.assert(reading_count == 1);
        } else {
            std.debug.assert(reading_count == 0);
        }

        std.debug.assert(reading_count <= 1);

        std.debug.assert(
            !(self.gzip_stream_finished and
                self.decompressor.hasPendingInput()),
        );

        std.debug.assert(
            !(self.processingFinished() and
                self.pending_copy_slot != null),
        );
    }

    fn logMetrics(self: *const Handler) void {
        std.log.info(
            \\ Successfully ingested...
            \\ Compressed: {d} MiB
            \\ Decompressed: {d} MiB
            \\ Disk reads: {d}
            \\ Inflate calls: {d}
            \\ COPY attemps: {d}
            \\ COPY would-block: {d}
            \\ POLL.OUT: {d}
        ,
            .{
                self.metris.compressed_bytes / (1024 * 1024),
                self.metris.decompressed_bytes / (1024 * 1024),
                self.metris.disk_read_count,
                self.metris.inflate_count,
                self.metris.copy_attempt_count,
                self.metris.copy_would_block_count,
                self.metris.poll_out_count,
            },
        );
    }
};
