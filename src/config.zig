const std = @import("std");

const Allocator = std.mem.Allocator;
const EnvMap = std.process.Environ.Map;

pub const Config = struct {
    pg_conn_info: [:0]const u8,
    pg_copy_query: [:0]const u8,
    file_path: []const u8,

    pub fn load(
        allocator: Allocator,
        env_map: *EnvMap,
    ) !Config {
        const conn_info = env_map.get("PIGEON_PG_CONN_INFO") orelse {
            std.log.err("PIGEON_PG_CONN_INFO", .{});
            return error.MissingEnvironmentVariable;
        };

        const source = env_map.get("PIGEON_SOURCE") orelse {
            std.log.err("PIGEON_SOURCE", .{});
            return error.MissingEnvironmentVariable;
        };

        const sink = env_map.get("PIGEON_SINK") orelse {
            std.log.err("PIGEON_SINK", .{});
            return error.MissingEnvironmentVariable;
        };

        const col_separator = env_map.get("PIGEON_COLUMN_SEPARATOR") orelse "|";

        const col_header = env_map.get("PIGEON_COLUMN_HEADER") orelse "true";
        const col_null = env_map.get("PIGEON_COLUMN_NULL") orelse "";

        const pg_conn_info = try allocator.dupeZ(u8, conn_info);

        const pg_copy_query = try std.fmt.allocPrintSentinel(
            allocator,
            \\ COPY 
            \\  {s}
            \\ FROM STDIN
            \\ WITH (
            \\  FORMAT csv,
            \\  DELIMITER '{s}',
            \\  HEADER {s},
            \\  NULL '{s}',
            \\  QUOTE E'\x01'
            \\ );
        ,
            .{
                sink,
                col_separator,
                col_header,
                col_null,
            },
            0,
        );

        return .{
            .pg_conn_info = pg_conn_info,
            .pg_copy_query = pg_copy_query,
            .file_path = source,
        };
    }
};
