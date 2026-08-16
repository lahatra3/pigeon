const std = @import("std");
const posix = std.posix;

const c = @import("c.zig").c;

pub const PutDataResult = enum(u8) {
    accepted,
    would_block,
};

pub const PgCopyIn = struct {
    conn_handle: *c.PGconn,
    socket: posix.fd_t,

    pub fn init(conn_info: [:0]const u8) !PgCopyIn {
        std.debug.assert(conn_info.len > 0);

        const conn = c.PQconnectdb(conn_info.ptr) orelse {
            std.log.err("[PostgreSQL]: connection memory allocation failed...", .{});
            return error.PostgresqlConnectionMemoryAllocationFailed;
        };
        errdefer c.PQfinish(conn);

        if (c.PQstatus(conn) != c.CONNECTION_OK) {
            std.log.err(
                \\ [PostgreSQL]: connection failed...
                \\  Error: {s}
            ,
                .{std.mem.span(c.PQerrorMessage(conn))},
            );
            c.PQfinish(conn);

            return error.PostgresqlConnectionFailed;
        }

        std.log.info("[PostgreSQL]: successfully connected...", .{});

        if (c.PQsetnonblocking(conn, 1) != 0) {
            std.log.err(
                \\ [PostgreSQL]: set non-blocking mode failed...
                \\  Error: {s}
            ,
                .{std.mem.span(c.PQerrorMessage(conn))},
            );

            return error.PostgresqlNonBlockingModeFailed;
        }

        const socket = c.PQsocket(conn);

        if (socket < 0) {
            return error.PostgresqlInvalidSocket;
        }

        return .{
            .conn_handle = conn,
            .socket = socket,
        };
    }

    pub fn deinit(self: *PgCopyIn) void {
        c.PQfinish(self.conn_handle);
        self.* = undefined;
    }

    pub fn startCopy(
        self: *PgCopyIn,
        query: [:0]const u8,
    ) !void {
        std.debug.assert(query.len > 0);

        if (c.PQsendQuery(self.conn_handle, query.ptr) == 0) {
            std.log.err(
                \\ [PostgreSQL]: sending query failed...
            ,
                .{std.mem.span(c.PQerrorMessage(self.conn_handle))},
            );

            return error.PostgresqlSendingQueryFailed;
        }

        try self.flushBlocking();

        while (c.PQisBusy(self.conn_handle) == 1) {
            try self.waitReadable();

            if (c.PQconsumeInput(self.conn_handle) == 0) {
                std.log.err(
                    \\ [PostgreSQL]: COPY_IN start response failed...
                    \\  Error: {s}
                ,
                    .{std.mem.span(c.PQerrorMessage(self.conn_handle))},
                );

                return error.PostgresqlReadFailed;
            }
        }

        const res = c.PQgetResult(self.conn_handle) orelse {
            std.log.err(
                "[PostgreSQL]: empty COPY_IN start response...",
                .{},
            );

            return error.PostgresqlCopyStartFailed;
        };
        defer c.PQclear(res);

        const status = c.PQresultStatus(res);

        if (status != c.PGRES_COPY_IN) {
            std.log.err(
                \\ [PostgreSQL]: COPY_IN could not start
                \\  Status: {d}
                \\  Error: {s}
            ,
                .{
                    status,
                    std.mem.span(c.PQresultErrorMessage(res)),
                },
            );

            return error.PostgresqlCopyStartFailed;
        }
    }

    pub fn putData(
        self: *PgCopyIn,
        data: []const u8,
    ) !PutDataResult {
        std.debug.assert(data.len > 0);

        if (data.len > std.math.maxInt(c_int)) {
            return error.PostgresqlCopyBlockTooLarge;
        }

        const status = c.PQputCopyData(
            self.conn_handle,
            @ptrCast(data.ptr),
            @intCast(data.len),
        );

        return switch (status) {
            1 => .accepted,
            0 => .would_block,
            else => {
                std.log.err(
                    \\ [PostgreSQL]: copying data failed...
                    \\  Error: {s} 
                ,
                    .{std.mem.span(c.PQerrorMessage(self.conn_handle))},
                );
            },
        };
    }

    pub fn flush(self: *PgCopyIn) !bool {
        return switch (c.PQflush(self.conn_handle)) {
            0 => true,
            1 => false,
            else => {
                std.log.err(
                    \\ [PostgreSQL]: flushing data failed...
                ,
                    .{std.mem.span(c.PQerrorMessage(self.conn_handle))},
                );

                return error.PostgresqlFlushingDataFailed;
            },
        };
    }

    pub fn consumeAvailableInput(self: *PgCopyIn) !void {
        if (c.PQconsumeInput(self.conn_handle) == 0) {
            std.log.err(
                \\ [PostgreSQL]: failed to consume data...
                \\  Error: {s}
            ,
                .{std.mem.span(c.PQerrorMessage(self.conn_handle))},
            );

            return error.PostgresqlSocketClosed;
        }

        while (c.PQisBusy(self.conn_handle) == 0) {
            const res = c.PQgetResult(self.conn_handle) orelse {
                break;
            };

            const status = c.PQresultStatus(res);

            if (status == c.PGRES_FATAL_ERROR) {
                std.log.info(
                    \\ [PostgreSQL]: COPY_IN failed...
                    \\  Error: {s}
                ,
                    .{std.mem.span(c.PQresultErrorMessage(res))},
                );

                c.PQclear(res);

                return error.PostgresqlCopyServerError;
            }

            c.PQclear(res);
        }
    }

    pub fn finalizeCopy(self: *PgCopyIn) !void {
        while (true) {
            const status = c.PQputCopyEnd(
                self.conn_handle,
                null,
            );

            if (status == 1) break;

            if (status < 1) {
                std.log.err(
                    \\ [PostgreSQL]: ending COPY_IN failed...
                    \\  Error: {s}
                ,
                    .{c.PQerrorMessage(self.conn_handle)},
                );

                return error.PostgresqlPutCopyEndFailed;
            }

            try self.flushBlocking();
        }

        try self.flushBlocking();

        while (c.PQisBusy(self.conn_handle) == 1) {
            try self.waitReadable();

            if (c.PQconsumeInput(self.conn_handle) == 0) {
                std.log.err(
                    \\ [PostgreSQL]: failed to receive final COPY_IN response...
                    \\  Error: {s}
                ,
                    .{std.mem.span(c.PQerrorMessage(self.conn_handle))},
                );

                return error.PostgresqlReadFailed;
            }
        }

        var command_ok_received = false;
        var result_count: u8 = 0;

        while (c.PQgetResult(self.conn_handle)) |res| {
            result_count += 1;

            if (result_count > 16) {
                c.PQclear(res);
                return error.PostgresqlTooManyResults;
            }

            const status = c.PQresultStatus(res);

            if (status == c.PGRES_COMMAND_OK) {
                command_ok_received = true;
            } else {
                std.log.err(
                    \\ [PostgreSQL]: COPY_IN finalisation failed...
                    \\  Status: {d}
                    \\  Error: {s}
                ,
                    .{
                        status,
                        std.mem.span(c.PQresultErrorMessage(res)),
                    },
                );

                c.PQclear(res);
                return error.PostgresqlCopyFailed;
            }
            
            c.PQclear(res);
        }

        if (!command_ok_received) {
            return error.PostgresqlCopyResultMissing;
        }
    }

    fn flushBlocking(self: *PgCopyIn) !void {
        const maximum_iterations: u32 = 1_000_000;
        var iteration: u32 = 0;

        while (!try self.flush()) : (iteration += 1) {
            if (iteration >= maximum_iterations) {
                return error.PostgresqlFlushIterationLimitExceeded;
            }

            try self.waitWritable();
        }
    }

    fn waitReadable(self: *PgCopyIn) !void {
        try self.waitFor(posix.POLL.IN);
    }

    fn waitWritable(self: *PgCopyIn) !void {
        try self.waitFor(posix.POLL.OUT);
    }

    fn waitFor(
        self: *PgCopyIn,
        events: i16,
    ) !void {
        var descriptors = [_]posix.pollfd{
            .{
                .fd = self.socket,
                .events = events,
                .revents = 0,
            },
        };

        while (true) {
            descriptors[0].revents = 0;
            
            const ready_count = posix.poll(
                descriptors[0..],
                -1,
            ) catch |err| switch (err) {
                error.Interrupted => continue,
                else => return err,
            };

            std.debug.assert(ready_count <= 1);

            if (ready_count == 0) continue;

            const returned = descriptors[0].revents;

            if ((returned & posix.POLL.NVAL) != 0) {
                return error.PostgresqlInvalidSocket;
            }

            if ((returned & posix.POLL.ERR) != 0) {
                std.log.err(
                    \\ [PostgreSQL]: socket poll error...
                    , .{},
                );

                return error.PostgresqlSocketPollFailed;
            }

            if ((returned & posix.POLL.HUP) != 0) {
                return error.PostgresqlSocketClosed;
            }

            if ((returned & events) != 0) {
                return;
            }
        }
    }
};
