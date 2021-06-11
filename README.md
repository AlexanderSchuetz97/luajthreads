# luajthreads
Library for luaj to do multithreading in lua

## License
Luajthreads is released under the GNU General Public License Version 3. <br>
A copy of the GNU General Public License Version 3 can be found in the COPYING file.<br>

## Dependencies
* Java 7 or newer
* luaj version 3.0.1

## Usage
Maven:
````
TODO
````

In Java:
````
Globals globals = JsePlatform.standardGlobals();
globals.load(new LuaThreadLib());
//.... (Standart LuaJ from this point)
globals.load(new InputStreamReader(new FileInputStream("test.lua")), "test.lua").call();
````
In test.lua:
````
local threads = require("threads")
--This function is called asynchronously in another thread.
--First parameter is always the execution environment
--All subsequent parameters can be used as you see fit.
function async(exc, param)
    --prints "Hello World" to STDOUT --
    print(param)
    print(exc:get("moreData"))
    -- do async computation here --
    return "RESULT"
end

--create the execution environment
--You can have multiple environments.
local execution = threads.execution()

--this is userdata instanceof java.lang.Callable and java.lang.Runnable
--it can also be used to interact with other java code via for example luajava lib.
--the callable will always return org.luaj.vm2.Varargs
--Runnable+Callable will only fail with LuaError
--The param is optional and you may have any number of params
local callable = execution:callable(mytest, "Hello World")

-- put/get can also be used to exchange more variables.
-- This is bidirectional and can even be used during execution
execution:put("moreData", "someData")

--This starts the callable
--This method can also be called with any java.lang.Runnable or java.lang.Callable userdata
local future = t.submit(callable)

-- do other stuff in the meantime

-- waits 2000 ms for the async function to complete and print "true RESULT" to STDOUT
-- true because the function did not take longer than 2000ms to finish and 
-- "RESULT" beeing the return value.
print(future:get(2000))
````
#### How to compile luajthreads
It is recommended to uncomment the maven-gpg-plugin section from the pom.xml
before building. Alternatively you may build it by passing "-Dgpg.skip" as a maven parameter.

If you do not want to run the junit tests then pass "-DskipTests"

#### Additional info and restrictions
1. The function passed to execution.callable must not use any upvalues 
(local variables) from outside the function.<br> 
If you pass a function that does so then an error is thrown by the execution.callable method.
2. Each thread has its own Globals environment. (Meaning all threads have different _G and global variables) <br>
A factory for this environment can be supplied in Java as a constructor parameter to LuaThreadLib
3. The lua debug api will see every thread as its own distinct script.
4. If the passed function is implemented in Java then it must have a ZeroArgConstructor and no variable named "u0" in its class.
5. For additional synchronization the library provides a TransferQueue and ReentrantLock implementation to lua.
    1) A TransferQueue may be created by calling threads.newQueue(). It has transfer, tryTransfer, add, poll, peek, take, size, clear methods exposed to lua.
    2) A ReentrantLock may be created by calling threads.newLock(). It has lock, unlock, tryLock, isLocked, isHeldByCurrentTrhead methods exposed to lua.
    3) All TimeUnit parameters from the corresponding java methods are always set to millisecond.
    4) Use execution:put/get to share those values between threads.
    5) Both values are userdata of their respective type and can be used as such with other java code.
    6) You may only insert Objects instanceof org.luaj.vm2.Varargs into the TransferQueue or errors are created when such objects are eventually withdrawn by lua. 
6. You may use coroutines normally inside threads. Be careful to lock and unlock Locks from 5. in the same coroutine otherwise deadlocks will occur.
7. In addition to the submit method, the schedule, scheduleAtFixedRate, scheduleWithFixedDelay methods are also available to run scheduled/repeating tasks.<br>
TimeUnit parameters of the corresponding java methods in ScheduledExecutorService are always set to millisecond.
8. luajthreads work with both LuaC and LuaJC. LuaJC requires the use of reflection to set the globals upval u0. This will work for luaj version 3.0.1 but may not work for different versions.
    1) The function execution.callable also accepts a string instead of a function as parameter. The string is assumed to either be a lua chunk from string.dump/luac or lua sourcode. This is always safe to use since the resulting function will always have no upvalues. This should be used when using LuaJC as no reflection is required in this case.
9. You may only share the following types (and varargs thereof) between threads using either parameters/return values/execution:put
    1) string
    2) number
    3) boolean
    4) nil
    5) userdata
        1) Only exception is the the excecution userdata.
10. Keep in mind that almost all libraries for luaj will not expect to be called concurrently. So think before you pass userdata between threads.