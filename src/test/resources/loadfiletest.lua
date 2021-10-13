local t = require("threads")

local exc = t.execution()
local runnable = exc:callable(loadfile("src/test/resources/loadfiletest_loaded.lua"))
local future = t.submit(runnable)
return future:get(2000)