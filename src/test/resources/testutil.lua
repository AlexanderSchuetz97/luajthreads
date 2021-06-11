local t = require("threads")
function mytest(exc, param)
    local lock = exc:get("lock")
    local queue = exc:get("queue")
    queue:add(exc:id())
    lock:lock()
    queue:transfer(param)
    lock:unlock()
end

local exc = t.execution()
local runnable = exc:callable(mytest, "Hello World")
local lock = t.lock()
local queue = t.queue()
exc:put("lock", lock)
exc:put("queue", queue)


lock:lock()
local future = t.submit(runnable)
if future:get(2000) then
    error("lock did not work")
end

local s, res = queue:poll(2000)
if not s or res ~= 1 then
    error("queue did not work")
end
lock:unlock()
s, res = queue:poll(2000)
if not s or res ~= "Hello World" then
    print(res)
    error("unlock/queue did not work")
end

if not future:get(2000) then
    error("future did not work")
end