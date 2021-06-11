local t = require("threads")

local someupval = 5
gvalue = 1
function noupval()
    return 5+5
end

function localvalue()
    local x = 1
    x = x + 1
    return x
end

function upvalue()
    return someupval + 1
end

function globalvalue()
    return gvalue
end

function nestedupvalue()
    local function nested()
        return someupval + 1
    end

    return nested()
end

function nestedlocalvalue()
    local x = 1
    local function nested()
        x = x + 1
        return x;
    end

    nested();
    x = x + 1
    return x
end

function nestedglobalvalue()
    local function nested()
        return gvalue
    end

    return nested()
end

function nestednonlocal()
    local x = 1
    function nested()
        return x
    end

    return nested()
end

local exc = t.execution()
function checkErr(func)
    if pcall(exc.callable, exc, func) then
        error("error expected")
    end
end

function checkNoErr(func, res)
    local c = exc:callable(func)
    local s, r = t.submit(c):get(2000)
    if not s then
        error("success expected")
    end

    if res ~= r then
        error("expected " .. tostring(res) .. " got " .. tostring(r))
    end

end


checkErr(upvalue)
checkErr(nestedupvalue)
checkNoErr(noupval, 10)
checkNoErr(localvalue, 2)
checkNoErr(globalvalue, nil)
checkNoErr(nestedlocalvalue, 3)
checkNoErr(nestedglobalvalue, nil)
checkNoErr(nestednonlocal, 1)