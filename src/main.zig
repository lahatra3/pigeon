const std = @import("std");
const Config = @import("config.zig").Config;
const Handler = @import("handler.zig").Handler;

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const allocator = init.arena.allocator();
    const env_map = init.environ_map;

    const config = try Config.load(
        allocator,
        env_map,
    );

    std.debug.assert(config.file_path.len > 0);
    std.debug.assert(config.pg_conn_info.len > 0);

    var handler = try Handler.init(
        io,
        allocator,
        config.file_path,
        config.pg_conn_info,
        16,
    );

    std.debug.assert(config.pg_copy_query.len > 0);
    
    try handler.run(config.pg_copy_query);
}
