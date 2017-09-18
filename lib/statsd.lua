local Statsd = {}

Statsd.time  = function (bucket, time, tags) Statsd.register(bucket, time .. "|ms", tags) end
Statsd.count = function (bucket, n, tags)    Statsd.register(bucket, n .. "|c", tags) end
Statsd.incr  = function (bucket, tags)       Statsd.count(bucket, 1, tags) end

Statsd.buffer = {} -- this table will be shared per worker process
                   -- if lua_code_cache is off, it will be cleared every request

Statsd.flush = function(sock, host, port)
   if sock then -- send buffer
      pcall(function()
               local udp = sock()
               udp:setpeername(host, port)
               udp:send(Statsd.buffer)
               udp:close()
            end)
   end

   -- empty buffer
   for k in pairs(Statsd.buffer) do Statsd.buffer[k] = nil end
end

Statsd.register = function (bucket, suffix, tags)
   local metric_part = bucket .. ":" .. suffix
   if tags then
               local tag_parts = {}
               for k, v in pairs(tags) do
                  table.insert(tag_parts, k .. ":" .. v)
               end
               table.insert(Statsd.buffer, metric_part .. "|#" .. table.concat(tag_parts, ",") .. "\n")
   else
               table.insert(Statsd.buffer, metric_part .. "\n")
   end
end

return Statsd
