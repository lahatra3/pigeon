const std = @import("std");
const Allocator = std.mem.Allocator;

pub const COMPRESSED_SLOT_COUNT: u8 = 2;
pub const DECOMPRESSED_SLOT_COUNT: u8 = 2;
const COMPRESSED_SLOT_SIZE_BYTES: u32 = 1024 * 1024;
const DECOMPRESSED_SLOT_SIZE_BYTES: u32 = 512 * 1024;

comptime {
    std.debug.assert(COMPRESSED_SLOT_COUNT == 2);
    std.debug.assert(DECOMPRESSED_SLOT_COUNT == 2);
    std.debug.assert(COMPRESSED_SLOT_SIZE_BYTES >= 64 * 1024);
    std.debug.assert(DECOMPRESSED_SLOT_SIZE_BYTES >= 64 * 1024);
    std.debug.assert(COMPRESSED_SLOT_SIZE_BYTES <= std.math.maxInt(u32));
    std.debug.assert(DECOMPRESSED_SLOT_SIZE_BYTES <= std.math.maxInt(u32));
}

pub const CompressedState = enum(u8) {
    free,
    reading,
    ready,
    inflating,
};

pub const DecompressedState = enum(u8) {
    free,
    filling,
    pending_copy,
};

pub const CompressedSlot = struct {
    buffer: []u8,
    length_bytes: u32 = 0,
    file_offset_bytes: u64 = 0,
    state: CompressedState = .free,
};

pub const DecompressedSlot = struct {
    buffer: []u8,
    length_bytes: u32 = 0,
    state: DecompressedState = .free,
};

pub const BufferPool = struct {
    allocator: Allocator,
    compressed: [COMPRESSED_SLOT_COUNT]CompressedSlot,
    decompressed: [DECOMPRESSED_SLOT_COUNT]DecompressedSlot,

    pub fn init(allocator: Allocator) !BufferPool {
        const compressed_0 = try allocator.alignedAlloc(
            u8,
            .@"4096",
            COMPRESSED_SLOT_SIZE_BYTES,
        );
        errdefer allocator.free(compressed_0);

        const compressed_1 = try allocator.alignedAlloc(
            u8,
            .@"4096",
            COMPRESSED_SLOT_SIZE_BYTES,
        );
        errdefer allocator.free(compressed_1);

        const decompressed_0 = try allocator.alignedAlloc(
            u8,
            .@"64",
            DECOMPRESSED_SLOT_SIZE_BYTES,
        );
        errdefer allocator.free(decompressed_0);

        const decompressed_1 = try allocator.alignedAlloc(
            u8,
            .@"64",
            DECOMPRESSED_SLOT_SIZE_BYTES,
        );
        errdefer allocator.free(decompressed_1);

        const pool = BufferPool{ .allocator = allocator, .compressed = .{ .{
            .buffer = compressed_0,
        }, .{
            .buffer = compressed_1,
        } }, .decompressed = .{ .{
            .buffer = decompressed_0,
        }, .{
            .buffer = decompressed_1,
        } } };

        pool.assertInvariants();

        return pool;
    }

    pub fn deinit(
        self: *BufferPool,
    ) void {
        self.assertInvariants();

        for (&self.compressed) |*slot| {
            self.allocator.free(slot.buffer);
            slot.* = undefined;
        }

        for (&self.decompressed) |*slot| {
            self.allocator.free(slot.buffer);
            self.* = undefined;
        }
    }

    pub inline fn compressedSlot(
        self: *BufferPool,
        index: u8,
    ) *CompressedSlot {
        std.debug.assert(index < COMPRESSED_SLOT_COUNT);

        return &self.compressed[index];
    }

    pub inline fn decompressedSlot(
        self: *BufferPool,
        index: u8,
    ) *DecompressedSlot {
        std.debug.assert(index < DECOMPRESSED_SLOT_COUNT);

        return &self.decompressed[index];
    }

    pub fn findFreeCompressed(self: *const BufferPool) ?u8 {
        var index: u8 = 0;

        while (index < COMPRESSED_SLOT_COUNT) : (index += 1) {
            if (self.compressed[index].state == .free) {
                return index;
            }
        }

        return null;
    }

    pub fn findReadyCompressed(
        self: *const BufferPool,
    ) ?u8 {
        var index: u8 = 0;

        while (index < COMPRESSED_SLOT_COUNT) : (index += 1) {
            if (self.compressed[index].state == .ready) {
                return index;
            }
        }

        return null;
    }

    pub fn findFreeDecompressed(
        self: *const BufferPool,
    ) ?u8 {
        var index: u8 = 0;

        while (index < DECOMPRESSED_SLOT_COUNT) : (index += 1) {
            if (self.decompressed[index].state == .free) {
                return index;
            }
        }

        return null;
    }

    pub fn releaseCompressed(
        self: *BufferPool,
        index: u8,
    ) void {
        std.debug.assert(index < COMPRESSED_SLOT_COUNT);

        const slot = self.compressedSlot(index);

        std.debug.assert(slot.state == .inflating);
        std.debug.assert(slot.length_bytes > 0);

        slot.length_bytes = 0;
        slot.file_offset_bytes = 0;
        slot.state = .free;

        std.debug.assert(slot.state == .free);
        std.debug.assert(slot.length_bytes == 0);
    }

    pub fn releaseDecompressed(
        self: *BufferPool,
        index: u8,
    ) void {
        std.debug.assert(index < DECOMPRESSED_SLOT_COUNT);

        const slot = self.decompressedSlot(index);

        std.debug.assert(slot.state == .pending_copy);
        std.debug.assert(slot.length_bytes == 0);

        slot.length_bytes = 0;
        slot.state = .free;

        std.debug.assert(slot.state == .free);
        std.debug.assert(slot.length_bytes == 0);
    }

    pub fn resetCompressedRead(
        self: *BufferPool,
        index: u8,
    ) void {
        std.debug.assert(index < COMPRESSED_SLOT_COUNT);

        const slot = self.compressedSlot(index);

        std.debug.assert(
            slot.state == .reading or
                slot.state == .ready,
        );

        slot.length_bytes = 0;
        slot.file_offset_bytes = 0;
        slot.state = .free;
    }

    pub fn resetDecompressedFill(
        self: *BufferPool,
        index: u8,
    ) void {
        std.debug.assert(index < DECOMPRESSED_SLOT_COUNT);

        const slot = self.decompressedSlot(index);

        std.debug.assert(slot.state == .filling);

        slot.length_bytes = 0;
        slot.state = .free;
    }

    pub fn assertInvariants(self: *BufferPool) void {
        for (self.compressed) |slot| {
            std.debug.assert(
                slot.buffer.len == COMPRESSED_SLOT_SIZE_BYTES,
            );

            std.debug.assert(
                slot.length_bytes <= COMPRESSED_SLOT_SIZE_BYTES,
            );

            if (slot.state == .free) {
                std.debug.assert(slot.length_bytes == 0);
            }

            if (slot.state == .ready or slot.state == .inflating) {
                std.debug.assert(slot.length_bytes > 0);
            }
        }

        for (self.decompressed) |slot| {
            std.debug.assert(
                slot.buffer.len == DECOMPRESSED_SLOT_SIZE_BYTES,
            );

            std.debug.assert(
                slot.length_bytes <= DECOMPRESSED_SLOT_SIZE_BYTES,
            );

            if (slot.state == .free) {
                std.debug.assert(slot.length_bytes == 0);
            }

            if (slot.state == .pending_copy) {
                std.debug.assert(slot.length_bytes > 0);
            }
        }
    }
};
