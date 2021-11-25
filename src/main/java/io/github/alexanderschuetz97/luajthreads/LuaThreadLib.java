//
// Copyright Alexander Schütz, 2021
//
// This file is part of luajthreads.
//
// luajthreads is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// luajthreads is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// A copy of the GNU Lesser General Public License should be provided
// in the COPYING & COPYING.LESSER files in top level directory of luajthreads.
// If not, see <https://www.gnu.org/licenses/>.
//
package io.github.alexanderschuetz97.luajthreads;

import io.github.alexanderschuetz97.luajthreads.userdata.LuaConditionUserdata;
import io.github.alexanderschuetz97.luajthreads.userdata.LuaExecutionUserdata;
import io.github.alexanderschuetz97.luajthreads.userdata.LuaFutureUserdata;
import io.github.alexanderschuetz97.luajthreads.userdata.LuaLockUserdata;
import io.github.alexanderschuetz97.luajthreads.userdata.LuaTransferQueueUserdata;
import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaClosure;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaFunction;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaUserdata;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Prototype;
import org.luaj.vm2.Upvaldesc;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.lib.OneArgFunction;
import org.luaj.vm2.lib.ThreeArgFunction;
import org.luaj.vm2.lib.TwoArgFunction;
import org.luaj.vm2.lib.VarArgFunction;
import org.luaj.vm2.lib.ZeroArgFunction;
import org.luaj.vm2.lib.jse.JsePlatform;
import org.luaj.vm2.luajc.LuaJC;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TransferQueue;

public class LuaThreadLib extends TwoArgFunction {

    /**
     * JsePlatform.standardGlobals + LuaThreadLib + undumper/loader/compiler from parent globals.
     */
    public static final GlobalFactory DEFAULT_GLOBALS_FACTORY = new GlobalFactory() {
        @Override
        public Globals createGlobals(Globals previousGlobals) {
            Globals globals = JsePlatform.standardGlobals();
            globals.compiler = previousGlobals.compiler;
            globals.loader = previousGlobals.loader;
            globals.undumper = previousGlobals.undumper;
            globals.load(new LuaThreadLib());
            return globals;
        }
    };

    protected final GlobalFactory factory;
    protected final ExecutorService executorService;
    protected final ScheduledExecutorService scheduledExecutorService;
    protected Globals globals;

    /**
     * Do not use this constructor for anything beyond simple things...
     */
    public LuaThreadLib() {
        this(DEFAULT_GLOBALS_FACTORY);
    }

    /**
     * Do not use this constructor for anything beyond simple things...
     */
    public LuaThreadLib(GlobalFactory factory) {
        this(factory, Executors.newCachedThreadPool(), null);
    }


    /**
     * If the supplied executor service is instanceof ScheduledExecutorService then it will be used for both purposes.
     */
    public LuaThreadLib(GlobalFactory factory, ExecutorService executorService) {
        this(factory, executorService, executorService instanceof ScheduledExecutorService ? (ScheduledExecutorService) executorService : null);
    }

    /**
     * The Global factory may not be null.
     */
    public LuaThreadLib(GlobalFactory factory, ExecutorService executorService, ScheduledExecutorService scheduledExecutorService) {
        this.executorService = executorService;
        this.scheduledExecutorService = scheduledExecutorService;
        this.factory = Objects.requireNonNull(factory);
    }

    /**
     * Called by Globals.load()
     */
    public LuaValue call(LuaValue arg1, LuaValue arg2) {
        globals = arg2.checkglobals();
        loadConditionBindings();
        loadExecutionBindings();
        loadFutureBindings();
        loadLockBindings();
        loadTransferQueueBinding();
        LuaValue libVal = createLib();
        arg2.get("package").get("loaded").set("threads", libVal);
        return libVal;
    }

    /**
     * Method binding for {@link LuaConditionUserdata}
     * Written once afterwards read by multiple threads.
     */
    protected final Map<LuaValue, LuaValue> conditionBinding = new HashMap<>();

    protected void loadConditionBindings() {
        conditionBinding.put(valueOf("await"), new TwoArgFunction() {
            @Override
            public LuaValue call(LuaValue arg, LuaValue arg2) {
                return conditionAwait(arg, arg2);
            }
        });

        conditionBinding.put(valueOf("signal"), new OneArgFunction() {
            @Override
            public LuaValue call(LuaValue arg) {
                return conditionSignal(arg);
            }
        });

        conditionBinding.put(valueOf("signalAll"), new OneArgFunction() {
            @Override
            public LuaValue call(LuaValue arg) {
                return conditionSignalAll(arg);
            }
        });
    }

    /**
     * Method binding for {@link LuaLockUserdata}
     * Written once afterwards read by multiple threads.
     */
    protected final Map<LuaValue, LuaValue> lockBinding = new HashMap<>();

    protected void loadLockBindings() {
        lockBinding.put(valueOf("lock"), new OneArgFunction() {

            public LuaValue call(LuaValue arg) {
                return lockLock(arg);
            }
        });

        lockBinding.put(valueOf("unlock"), new OneArgFunction() {

            public LuaValue call(LuaValue arg) {
                return lockUnlock(arg);
            }
        });

        lockBinding.put(valueOf("isHeldByCurrentThread"), new OneArgFunction() {
            @Override
            public LuaValue call(LuaValue arg) {
                return lockIsHeldByCurrentThread(arg);
            }
        });

        lockBinding.put(valueOf("isLocked"), new OneArgFunction() {
            @Override
            public LuaValue call(LuaValue arg) {
                return lockIsLocked(arg);
            }
        });

        lockBinding.put(valueOf("tryLock"), new TwoArgFunction() {
            public LuaValue call(LuaValue arg1, LuaValue arg2) {
               return lockTryLock(arg1, arg2);
            }
        });

        lockBinding.put(valueOf("condition"), new OneArgFunction() {
            @Override
            public LuaValue call(LuaValue arg) {
                return newCondition(arg);
            }
        });
    }

    /**
     * Method binding for {@link LuaExecutionUserdata}
     * Written once afterwards read by multiple threads.
     */
    public final Map<LuaValue, LuaValue> executionBinding = new HashMap<>();

    protected void loadExecutionBindings() {
        executionBinding.put(valueOf("id"), new OneArgFunction() {
            public LuaValue call(LuaValue arg) {
                return executionGetID(arg);
            }
        });
        executionBinding.put(valueOf("parent"), new OneArgFunction() {
            public LuaValue call(LuaValue arg) {
                return executionGetParent(arg);
            }
        });

        executionBinding.put(valueOf("keys"), new OneArgFunction() {
            @Override
            public LuaValue call(LuaValue arg) {
                return executionGetKeys(arg);
            }
        });

        executionBinding.put(valueOf("clear"), new OneArgFunction() {
            @Override
            public LuaValue call(LuaValue arg) {
                return executionClear(arg);
            }
        });

        executionBinding.put(valueOf("put"), new ThreeArgFunction() {
            @Override
            public LuaValue call(LuaValue arg, LuaValue arg2, LuaValue arg3) {
                return executionPut(arg, arg2, arg3);
            }
        });

        executionBinding.put(valueOf("putIfAbsent"), new ThreeArgFunction() {
            @Override
            public LuaValue call(LuaValue arg, LuaValue arg2, LuaValue arg3) {
                return executionPutIfAbsent(arg, arg2, arg3);
            }
        });

        executionBinding.put(valueOf("get"), new TwoArgFunction() {
            @Override
            public LuaValue call(LuaValue arg, LuaValue arg2) {
                return executionGet(arg, arg2);
            }
        });

        executionBinding.put(valueOf("callable"), new VarArgFunction() {
            @Override
            public Varargs invoke(Varargs args) {
                return newCallable(args);
            }

        });

        executionBinding.put(valueOf("join"), new TwoArgFunction() {
            @Override
            public LuaValue call(LuaValue arg, LuaValue arg2) {
               return executionJoin(arg, arg2);
            }
        });

        executionBinding.put(valueOf("running"), new TwoArgFunction() {
            @Override
            public LuaValue call(LuaValue arg, LuaValue arg2) {
                return executionRunning(arg, arg2);
            }
        });
    }

    /**
     * Method binding for {@link LuaFutureUserdata}
     * Written once afterwards read by multiple threads.
     */
    public final Map<LuaValue, LuaValue> futureBinding = new HashMap<>();

    protected void loadFutureBindings() {
        futureBinding.put(valueOf("isDone"), new OneArgFunction() {
            @Override
            public LuaValue call(LuaValue arg) {
                return futureIsDone(arg);
            }
        });

        futureBinding.put(valueOf("isCancelled"), new OneArgFunction() {
            @Override
            public LuaValue call(LuaValue arg) {
                return futureIsCancelled(arg);
            }
        });

        futureBinding.put(valueOf("get"), new VarArgFunction() {
            @Override
            public Varargs invoke(Varargs arg) {
                return futureGet(arg);
            }
        });

        futureBinding.put(valueOf("cancel"), new TwoArgFunction() {
            @Override
            public LuaValue call(LuaValue arg, LuaValue arg2) {
                return futureCancel(arg, arg2);
            }
        });


    }

    /**
     * Method binding for {@link LuaTransferQueueUserdata}
     * Written once afterwards read by multiple threads.
     */
    public final Map<LuaValue, LuaValue> transferQueueBinding = new HashMap<>();

    protected void loadTransferQueueBinding() {
        transferQueueBinding.put(valueOf("peek"), new VarArgFunction() {
            @Override
            public Varargs invoke(Varargs arg) {
               return queuePeek(arg);
            }
        });

        transferQueueBinding.put(valueOf("poll"), new VarArgFunction() {
            @Override
            public Varargs invoke(Varargs arg) {
                return queuePoll(arg);
            }
        });

        transferQueueBinding.put(valueOf("take"), new VarArgFunction() {
            @Override
            public Varargs invoke(Varargs arg) {
                return queueTake(arg);
            }
        });

        transferQueueBinding.put(valueOf("size"), new OneArgFunction() {
            @Override
            public LuaValue call(LuaValue arg) {
                return queueSize(arg);
            }
        });

        transferQueueBinding.put(valueOf("add"), new VarArgFunction() {
            @Override
            public Varargs invoke(Varargs arg) {
                return queueAdd(arg);
            }
        });

        transferQueueBinding.put(valueOf("transfer"), new VarArgFunction() {
            @Override
            public Varargs invoke(Varargs arg) {
                return queueTransfer(arg);
            }
        });

        transferQueueBinding.put(valueOf("tryTransfer"), new VarArgFunction() {
            @Override
            public Varargs invoke(Varargs arg) {
                return queueTryTransfer(arg);
            }
        });

        transferQueueBinding.put(valueOf("clear"), new OneArgFunction() {
            @Override
            public LuaValue call(LuaValue arg) {
                return queueClear(arg);
            }
        });

        transferQueueBinding.put(valueOf("isEmpty"), new OneArgFunction() {
            @Override
            public LuaValue call(LuaValue arg) {
                return queueIsEmpty(arg);
            }
        });
    }

    /**
     * Function to wire the library table together.
     * Keep in mind that most functions behave like a pseudo tables in that you can also get the binding functions
     * with the get method. This can be used when the userdata is supplied from external java code.
     * Ex lua code: require('threads').lock.unlock(myLockFromJava)
     */
    protected LuaValue createLib() {
        LuaTable table = new LuaTable();
        table.set("lock", new ZeroArgFunction() {
            @Override
            public LuaValue call() {
                return newLock();
            }

            @Override
            public LuaValue get(LuaValue key) {
                LuaValue bound = lockBinding.get(key);
                if (bound == null) {
                    return super.get(key);
                }

                return bound;
            }
        });

        table.set("condition", new OneArgFunction() {
            @Override
            public LuaValue call(LuaValue arg) {
                return newCondition(arg);
            }

            @Override
            public LuaValue get(LuaValue key) {
                LuaValue bound = conditionBinding.get(key);
                if (bound == null) {
                    return super.get(key);
                }

                return bound;
            }
        });

        table.set("execution", new ZeroArgFunction() {
            @Override
            public LuaValue call() {
                return newExecution();
            }

            @Override
            public LuaValue get(LuaValue key) {
                LuaValue bound = executionBinding.get(key);
                if (bound == null) {
                    return super.get(key);
                }

                return bound;
            }
        });

        table.set("queue", new ZeroArgFunction() {
            @Override
            public LuaValue call() {
                return newQueue();
            }

            @Override
            public LuaValue get(LuaValue key) {
                LuaValue bound = transferQueueBinding.get(key);
                if (bound == null) {
                    return super.get(key);
                }

                return bound;
            }
        });

        table.set("hasExecutor", new ZeroArgFunction() {
            @Override
            public LuaValue call() {
                return valueOf(executorService != null);
            }
        });

        table.set("hasScheduler", new ZeroArgFunction() {
            @Override
            public LuaValue call() {
                return valueOf(scheduledExecutorService != null);
            }
        });

        table.set("submit", new OneArgFunction() {
            @Override
            public LuaValue call(LuaValue arg) {
                return submit(arg);
            }
        });

        table.set("schedule", new TwoArgFunction() {
            @Override
            public LuaValue call(LuaValue arg1, LuaValue arg2) {
                return schedule(arg1, arg2);
            }
        });


        table.set("scheduleAtFixedRate", new ThreeArgFunction() {
            @Override
            public LuaValue call(LuaValue arg1, LuaValue arg2, LuaValue arg3) {
                return scheduleAtFixedRate(arg1, arg2, arg3);
            }
        });

        table.set("scheduleWithFixedDelay", new ThreeArgFunction() {
            @Override
            public LuaValue call(LuaValue arg1, LuaValue arg2, LuaValue arg3) {
                return scheduleWithFixedDelay(arg1, arg2, arg3);
            }
        });

        table.set("sleep", new OneArgFunction() {
            @Override
            public LuaValue call(LuaValue arg) {
                return sleep(arg);
            }
        });

        table.set("getName", new ZeroArgFunction() {
            @Override
            public LuaValue call() {
                return getThreadName();
            }
        });

        table.set("setName", new OneArgFunction() {
            @Override
            public LuaValue call(LuaValue value) {
                return setThreadName(value);
            }
        });

        table.set("id", new ZeroArgFunction() {
            @Override
            public LuaValue call() {
                return getThreadID();
            }
        });

        return table;
    }

    //QUEUE
    protected Varargs queuePeek(Varargs arg) {
        Object obj = Util.checkTransferQueue(arg.arg1()).peek();

        if (obj == null) {
            return FALSE;
        }

        return varargsOf(TRUE, Util.checkQueueResult(obj));
    }

    protected Varargs queuePoll(Varargs arg) {
        TransferQueue queue = Util.checkTransferQueue(arg.arg1());
        Object obj;
        if (arg.isnoneornil(2)) {
            obj = queue.poll();
        } else {
            try {
                obj = queue.poll(arg.checklong(2), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                return Util.interrupted();
            }
        }

        if (obj == null) {
            return FALSE;
        }

        return varargsOf(TRUE, Util.checkQueueResult(obj));
    }

    protected Varargs queueTake(Varargs arg) {
        TransferQueue queue = Util.checkTransferQueue(arg.arg1());
        Object obj;
        try {
            obj = queue.take();
        } catch (InterruptedException e) {
            return Util.interrupted();
        }

        return Util.checkQueueResult(obj);
    }
    protected LuaValue queueSize(LuaValue arg) {
        return valueOf(Util.checkTransferQueue(arg.arg1()).size());
    }
    protected Varargs queueAdd(Varargs arg) {
        try {
            Util.checkTransferQueue(arg.arg1()).add(Util.checkValues(arg.subargs(2)));
        } catch (IllegalStateException exc) {
            return error("queue full");
        }

        return NONE;
    }
    protected Varargs queueTransfer(Varargs arg) {
        try {
            Util.checkTransferQueue(arg.arg1()).transfer(Util.checkValues(arg.subargs(2)));
        } catch (InterruptedException e) {
            Util.interrupted();
        }
        return NONE;
    }
    protected Varargs queueTryTransfer(Varargs arg) {
        try {
            return valueOf(Util.checkTransferQueue(arg.arg1()).tryTransfer(Util.checkValues(arg.subargs(3)), arg.checkint(2), TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {
            Util.interrupted();
        }
        return NONE;
    }
    protected LuaValue queueClear(LuaValue arg) {
        Util.checkTransferQueue(arg.arg1()).clear();
        return NONE;
    }

    protected LuaValue queueIsEmpty(LuaValue arg) {
        return valueOf(Util.checkTransferQueue(arg.arg1()).isEmpty());
    }

    //FUTURE
    protected LuaValue futureIsDone(LuaValue arg) {
        return valueOf(Util.checkFuture(arg).isDone());
    }

    protected LuaValue futureIsCancelled(LuaValue arg) {
        return valueOf(Util.checkFuture(arg).isCancelled());
    }

    protected Varargs futureGet(Varargs arg) {
        Object res;

        try {
            if (arg.isnoneornil(2)) {
                res = Util.checkFuture(arg.arg1()).get();
            } else {
                res = Util.checkFuture(arg.arg1()).get(arg.checklong(2), TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException e) {
            return Util.interrupted();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof LuaError) {
                throw (LuaError) e.getCause();
            }
            throw new LuaError(e.getCause() == null ? e : e.getCause());
        } catch (TimeoutException e) {
            return FALSE;
        }

        if (res == null) {
            return TRUE;
        }

        if (res instanceof Varargs) {
            return varargsOf(TRUE, (Varargs) res);
        }

        return varargsOf(TRUE, coerce(res));
    }

    protected LuaValue futureCancel(LuaValue arg, LuaValue arg2) {
        return valueOf(Util.checkFuture(arg).cancel(!arg2.isnil() && arg2.checkboolean()));
    }

    //Execution
    protected LuaValue executionGetID(LuaValue arg) {
        return valueOf(Util.checkExecution(arg).getID());
    }

    protected LuaValue executionGetParent(LuaValue arg) {
        return valueOf(Util.checkExecution(arg).getParentID());
    }

    protected LuaValue executionPut(LuaValue arg, LuaValue arg2, LuaValue arg3) {
        return Util.checkExecution(arg).putSharedValues(arg2, arg3);
    }

    protected LuaValue executionClear(LuaValue arg) {
        Util.checkExecution(arg).clearSharedValues();
        return NONE;
    }

    protected LuaValue executionGetKeys(LuaValue arg) {
        return Util.checkExecution(arg).getSharedValueKeys();
    }

    protected LuaValue executionPutIfAbsent(LuaValue arg, LuaValue arg2, LuaValue arg3) {
        return Util.checkExecution(arg).putSharedValueIfAbsent(arg2, arg3);
    }

    protected LuaValue executionGet(LuaValue arg, LuaValue arg2) {
        return Util.checkExecution(arg).getSharedValue(arg2);
    }

    protected Varargs newCallable(Varargs args) {
        return new LuaUserdata(Util.checkExecution(args.arg1()).createCallable(args.arg(2), args.subargs(3)));
    }

    protected LuaValue executionJoin(LuaValue arg, LuaValue arg2) {
        Util.checkExecution(arg).await(arg2.checklong());
        return NIL;
    }


    protected LuaValue executionRunning(LuaValue arg, LuaValue arg2) {
        return valueOf(Util.checkExecution(arg).running(arg2.checklong()));
    }


    //Condition
    protected LuaValue conditionAwait(LuaValue arg, LuaValue arg2) {
        if (arg2.isnil()) {
            try {
                Util.checkCondition(arg).await();
                return TRUE;
            } catch (IllegalMonitorStateException e) {
                return error("lock not held");
            } catch (InterruptedException e) {
                return Util.interrupted();
            }
        }

        try {
            return valueOf(Util.checkCondition(arg).await(arg2.checkint(), TimeUnit.MILLISECONDS));
        } catch (IllegalMonitorStateException e) {
            return error("lock not held");
        } catch (InterruptedException e) {
            return Util.interrupted();
        }
    }


    protected LuaValue conditionSignal(LuaValue arg) {
        try {
            Util.checkCondition(arg).signal();
        } catch (IllegalMonitorStateException e) {
            return error("lock not held");
        }

        return NONE;
    }


    protected LuaValue conditionSignalAll(LuaValue arg) {
        try {
            Util.checkCondition(arg).signalAll();
        } catch (IllegalMonitorStateException e) {
            return error("lock not held");
        }

        return NONE;
    }

    //LOCK
    protected LuaValue newCondition(LuaValue lock) {
        return new LuaConditionUserdata(conditionBinding, Util.checkLock(lock).newCondition());
    }


    protected LuaValue lockLock(LuaValue arg) {
        Util.checkLock(arg).lock();
        return NIL;
    }

    protected LuaValue lockUnlock(LuaValue arg) {
        try {
            Util.checkLock(arg).unlock();
        } catch (IllegalMonitorStateException exc) {
            return error("lock not held");
        }
        return NIL;
    }

    protected LuaValue lockIsHeldByCurrentThread (LuaValue arg) {
        return valueOf(Util.checkLock(arg).isHeldByCurrentThread());
    }

    protected LuaValue lockIsLocked(LuaValue arg) {
        return valueOf(Util.checkLock(arg).isLocked());
    }

    protected LuaValue lockTryLock(LuaValue arg1, LuaValue arg2) {
        if (arg2.isnil()) {
            return valueOf(Util.checkLock(arg1).tryLock());
        }
        try {
            return valueOf(Util.checkLock(arg1).tryLock(arg2.checkint(), TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {
            throw new LuaError("interrupted");
        }
    }

    ///EXECUTOR

    protected LuaValue sleep(LuaValue value) {
        try {
            Thread.sleep(value.checklong());
        } catch (InterruptedException e) {
            Util.interrupted();
        }
        return NONE;
    }

    protected LuaValue getThreadName() {
        return valueOf(Thread.currentThread().getName());
    }

    protected LuaValue setThreadName(LuaValue value) {
        Thread.currentThread().setName(value.checkstring().checkjstring());
        return NONE;

    }

    protected LuaValue getThreadID() {
        return valueOf(Thread.currentThread().getId());
    }

    protected LuaValue submit(LuaValue arg) {
        if (executorService == null) {
            return Util.noExecutor();
        }

        Object ud = arg.checkuserdata();
        if (ud instanceof Callable) {
            return new LuaFutureUserdata(futureBinding, executorService.submit((Callable<?>) ud));
        }

        if (ud instanceof Runnable) {
            return new LuaFutureUserdata(futureBinding, executorService.submit((Runnable) ud));
        }

        return error("expected runnable or callable got " + ud.getClass().getName());
    }

    protected LuaValue schedule(LuaValue arg, LuaValue delay) {
        if (scheduledExecutorService == null) {
            return Util.noScheduler();
        }

        Object ud = arg.checkuserdata();
        if (ud instanceof Runnable) {
            return new LuaFutureUserdata(futureBinding, scheduledExecutorService.schedule((Runnable) ud, delay.checklong(), TimeUnit.MILLISECONDS));
        }

        if (ud instanceof Callable) {
            return new LuaFutureUserdata(futureBinding, scheduledExecutorService.schedule((Callable<?>) ud, delay.checklong(), TimeUnit.MILLISECONDS));
        }

        return error("expected runnable or callable got " + ud.getClass().getName());
    }

    protected LuaValue scheduleAtFixedRate(LuaValue arg, LuaValue delay, LuaValue period) {
        if (scheduledExecutorService == null) {
            return Util.noScheduler();
        }

        scheduledExecutorService.scheduleAtFixedRate((Runnable) arg.checkuserdata(Runnable.class), delay.checklong(), period.checklong(), TimeUnit.MILLISECONDS);
        return NONE;
    }

    protected LuaValue scheduleWithFixedDelay(LuaValue arg, LuaValue delay, LuaValue period) {
        if (scheduledExecutorService == null) {
            return Util.noScheduler();
        }

        scheduledExecutorService.scheduleWithFixedDelay((Runnable) arg.checkuserdata(Runnable.class), delay.checklong(), period.checklong(), TimeUnit.MILLISECONDS);
        return NONE;
    }

    protected LuaValue newQueue() {
        return new LuaTransferQueueUserdata(transferQueueBinding);
    }

    protected LuaValue newLock() {
        return new LuaLockUserdata(lockBinding);
    }

    protected LuaValue newExecution() {
        //This is the root execution id 0 -> no parent.
        return new LuaExecutionUserdata(executionBinding, new LuaExecutor(this).newExecution(null));
    }

    protected <X extends Callable<Varargs> & Runnable> X createRunnable(final LuaExecution parent, final LuaValue torun, final Varargs varargs) {
        for (int i = 1; i <= varargs.narg(); i++) {
            Util.checkValue(varargs.arg(i));
        }

        if (torun.isclosure()) {
            LuaClosure closure = (LuaClosure) torun;
            checkClosureStack(closure);
            return (X) new LuaRunnableWithPrototype(parent, closure.p, varargs);
        }

        if (torun.isfunction()) {
            final LuaFunction function = torun.checkfunction();
            if (checkJavaStack(function)) {
                return (X) new LuaRunnableWithSafeUpValues(parent, function.getClass(), varargs);
            }
            return (X) new LuaRunnableWithUnsafeUpValues(parent, function.getClass(), varargs);
        }

        LuaString code = torun.checkstring();

            Prototype pt;
            try {
                pt = globals.loadPrototype(new ByteArrayInputStream(code.m_bytes, code.m_offset, code.m_length), "runnable", "bt");
            } catch (IOException e) {
                throw new LuaError(e);
            }

        if (!useLuaJC()) {
            return (X) new LuaRunnableWithPrototype(parent, pt, varargs);
        }

        Class<? extends LuaFunction> functionClass;
        try {
            functionClass = LuaJC.instance.load(pt, "runnable", globals).getClass();
        } catch (IOException e) {
            throw new LuaError(e);
        }

        return (X) new LuaRunnableWithSafeUpValues(parent, functionClass, varargs);
    }

    //UTIL

    /**
     * Environments that use luajava may want to overwrite this and use {@link org.luaj.vm2.lib.jse.CoerceJavaToLua} instead of just returning new LuaUserdata.
     */
    protected Varargs coerce(Object object) {
        if (object instanceof Varargs) {
            return (Varargs) object;
        }

        return new LuaUserdata(object);
    }

    protected boolean useLuaJC() {
        return globals.loader == LuaJC.instance;
    }

    protected LuaFunction loadUnsafeUpValueFunctionWithNewGlobals(Class<? extends LuaFunction> function) {
        try {
            LuaFunction loaded = function.getConstructor().newInstance();
            Globals newGlobals = factory.createGlobals(globals);
            loaded.initupvalue1(newGlobals);
            Field f = function.getDeclaredField("u0");
            f.setAccessible(true);
            f.set(loaded, newGlobals);
            return loaded;
        } catch (Exception e) {
            throw new LuaError("invalid function");
        }
    }

    protected LuaFunction loadFunctionWithNewGlobals(Class<? extends LuaFunction> function) {
        try {
            LuaFunction loaded = function.getConstructor().newInstance();
            Globals newGlobals = factory.createGlobals(globals);
            loaded.initupvalue1(newGlobals);
            return loaded;
        } catch (Exception e) {
            throw new LuaError("invalid function");
        }
    }


    protected LuaFunction loadPrototypeWithNewGlobals(Prototype prototype) {
        return new LuaClosure(prototype, factory.createGlobals(globals));
    }

    protected boolean checkJavaStack(LuaFunction function) {
        Class<? extends LuaFunction> functionClass = function.getClass();
        try {
            functionClass.getConstructor();
        } catch (NoSuchMethodException e) {
            throw new LuaError("function does not have a zero arg constructor");
        }

        //LuaJC is a bit tricky because we do not have the info from the original prototype anymore...
        //Meaning we have to resort to reflection with is unfortunate...
        try {

            Field upvalue0;
            try {
                upvalue0 = functionClass.getDeclaredField("u0");
            } catch (NoSuchFieldException exc) {
                //NO upvalues at all or some user function okay I guess...
                return true;
            }
            upvalue0.setAccessible(true);
            Object upValue = upvalue0.get(function);
            if (!(upValue instanceof Globals)) {
                throw new LuaError("function is referencing upvalues");
            }

            try {
                functionClass.getDeclaredField("u1");
                throw new LuaError("function is referencing upvalues");
            } catch (NoSuchFieldException exc) {
                //NO non global upvalues this is what we need.
                return false;
            }
        } catch (IllegalAccessException | SecurityException e) {
            throw new LuaError("error checking upvalues in " + function.getClass().getName());
        }
    }

    protected void checkClosureStack(LuaClosure closure) {
        Upvaldesc[] upvaldesc = closure.p.upvalues;
        if (upvaldesc.length == 0) {
            return;
        }



        if (upvaldesc.length == 1) {
            if (upvaldesc[0].instack) {
                //loadfile loads a function with 1 upval called _ENV which is equal to _G and filled by closure constructor
                if (closure.upValues[0].getValue() instanceof Globals) {
                    return;
                }
                throw new LuaError("function is referencing upvalues called " + upvaldesc[0].name);
            }
            return;
        }

        //Since we have this info may as well use it...
        StringBuilder variables = new StringBuilder();
        for (int i = 1; i < upvaldesc.length; i++) {
            if (i != 1) {
                variables.append(", ");
            }
            variables.append(upvaldesc[i].name);
        }
        throw new LuaError("function is referencing upvalues called " + variables);

    }

    protected abstract class AbstractLuaRunnable implements Callable<Varargs>, Runnable {

        final LuaExecution parent;
        final Varargs varargs;

        protected AbstractLuaRunnable(LuaExecution parent, Varargs varargs) {
            this.parent = parent;
            this.varargs = varargs;
        }

        public abstract LuaFunction loadFunction();

        @Override
        public void run() {
            call();
        }

        @Override
        public Varargs call() {
            LuaFunction clone = loadFunction();
            LuaExecution execution = parent.newExecution();
            execution.signalStart();
            try {
                return Util.checkValues(clone.invoke(varargsOf(new LuaExecutionUserdata(executionBinding, execution), varargs)));
            } finally {
                execution.signalEnd();
            }
        }
    }

    protected class LuaRunnableWithPrototype extends AbstractLuaRunnable {
        final Prototype function;

        public LuaRunnableWithPrototype(LuaExecution parent, Prototype function, Varargs varargs) {
            super(parent, varargs);
            this.function = function;
        }

        @Override
        public LuaFunction loadFunction() {
            return loadPrototypeWithNewGlobals(function);
        }
    }

    protected class LuaRunnableWithSafeUpValues extends AbstractLuaRunnable {

        final Class<? extends LuaFunction> function;

        public LuaRunnableWithSafeUpValues(LuaExecution parent, Class<? extends LuaFunction> function, Varargs varargs) {
            super(parent, varargs);
            this.function = function;
        }

        @Override
        public LuaFunction loadFunction() {
            return loadFunctionWithNewGlobals(function);
        }
    }

    protected class LuaRunnableWithUnsafeUpValues extends AbstractLuaRunnable {

        final Class<? extends LuaFunction> function;

        public LuaRunnableWithUnsafeUpValues(LuaExecution parent, Class<? extends LuaFunction> function, Varargs varargs) {
            super(parent, varargs);
            this.function = function;
        }

        @Override
        public LuaFunction loadFunction() {
            return loadUnsafeUpValueFunctionWithNewGlobals(function);
        }
    }

    public interface GlobalFactory {
        /**
         * This method is already called in the new thread that will run the async function.
         * Do NOT copy fields over from the previousGlobals.
         *
         * The are only supplied incase your application uses a custom GlobalsSubclass which ofc can have variables that are
         * threadsafe and be copied over.
         * The default implementation uses this to copy over the compiler/loader/undumper from the previous environment.
         */
        Globals createGlobals(Globals previousGlobals);
    }


}
