# openresty-statsd [![travis-ci](https://secure.travis-ci.org/lonelyplanet/openresty-statsd.png)](https://secure.travis-ci.org/lonelyplanet/openresty-statsd)

A Lua module for openresty to send metrics to StatsD

## Features

* increments!
* counts!
* timers!
* cosocket frolics
* batchin'

## Installation

1. [Install openresty](http://openresty.org) configured `--with-luajit`
2. Copy `lib/statsd.lua` somewhere that openresty nginx can find (you may need to adjust your LUA_PATH or use `lua_package_path` [directive](http://wiki.nginx.org/HttpLuaModule#lua_package_path)
3. Configure nginx:

```
    # an nginx conf
    http {
      -- optionally set relative lua_package_path
      lua_package_path "${prefix}lua/*.lua";
      
      -- make the statsd variable available in each phase
      init_by_lua 'statsd = require("statsd")';

      location /some_location {
        content_by_lua '
          -- this is the phase where metrics are sent
          -- batch metrics into packets of at least 50
          if table.getn(statsd.buffer) > 50 then statsd.flush(ngx.socket.udp, "127.0.0.1", 8125) end
        ';
        
        log_by_lua '
          -- this is the phase where metrics are registered
          statsd.incr("test.status." .. ngx.var.status)
          statsd.time("test.req_time", ngx.now() - ngx.req.start_time())
        ';
      }
    }
```

The request-response lifecycle in nginx has [eight phases](http://wiki.nginx.org/HttpLuaModule#ngx.get_phase). The data you are likely to want to report (HTTP status, request time) is available in the last phase, `log`, but the socket API is not available. That's why stats are registered in `log_by_lua` and sent via `flush` in `content_by_lua`.

## Changelog

* 0.0.1: Works. Tested.  

## Development

### Prerequisites for dev and testing

* http://openresty.org/#Installation
* https://github.com/etsy/statsd#installation-and-configuration
* ruby, rubygems, bundler
* luarocks

### Build

1. Clone the repo
2. `bundle`
3. `rake openresty:install`
4. `rake statsd:install`
5. `luarocks install busted [--local]`
6. Add `lib/?.lua` to your `LUA_PATH`

`guard` will run the unit tests.

`foreman start` will spin up nginx and statsd using dev configuration from `./config` and `./examples`.

## Related projects

* [lua-statsd](https://github.com/cwarden/lua-statsd) - doesn't use openresty's cosockets
* [nginx-statsd](https://github.com/zebrafishlabs/nginx-statsd) - written in C
* [fozzie](https://github.com/lonelyplanet/fozzie) - our Ruby StatsD client

## Author

[Dave Nolan](https://github.com/textgoeshere) / [lonelyplanet.com](http://www.lonelyplanet.com)
