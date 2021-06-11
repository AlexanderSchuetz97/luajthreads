local t = require("threads")
function mytest(exc, param)
    return exc:id() .. " " .. exc:parent() .. " " .. param;
end

local exc = t.execution()
local runnable = exc:callable(mytest, "Hello World")
local future = t.submit(runnable)
return future:get(2000)