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

import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;

import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * This object represents a lua thread inside a executor environment.
 * It has access to its own id aswell as the executor environment.
 *
 * The signalStart and signalEnd methods should not be called from lua as they signal to the executor
 * that the state of this execution changed.
 */
public class LuaExecution {

    private final LuaExecutor executor;
    private final long id;
    private final long parentID;
    private boolean complete = false;
    private final Object mutex = new Object();

    public LuaExecution(LuaExecutor executor, long id, long parent) {
        this.executor = Objects.requireNonNull(executor);
        this.id = id;
        this.parentID = parent;
    }

    public long getID() {
        return id;
    }

    public long getParentID() {
        return parentID;
    }

    /**
     * Called by lua to set a shared variable. Returns the previous value. This is an atomic swap.
     */
    public LuaValue putSharedValues(LuaValue aKey, LuaValue aValue) {
        return executor.putSharedValues(aKey, aValue);
    }

    /**
     * Called by lua to set a shared variable if absent. This is atomic.
     * Returns the old value or NIL.
     */
    public LuaValue putSharedValueIfAbsent(LuaValue aKey, LuaValue aValue) {
        return executor.putSharedValueIfAbsent(aKey, aValue);
    }

    /**
     * Called by lua to get a shared variable
     */
    public LuaValue getSharedValue(LuaValue aKey) {
        return executor.getSharedValue(aKey);
    }

    /**
     * Called by lua to get all shared value keys as a table.
     */
    public LuaValue getSharedValueKeys() {
        return executor.getSharedValueKeys();
    }

    /**
     * Called by lua to clear all shared values.
     */
    public void clearSharedValues() {
        executor.clearSharedValues();
    }

    /**
     * Called by lua to wrap a function into a callable.
     */
    public Callable<Varargs> createCallable(LuaValue function, Varargs varargs) {
        return executor.getThreadLib().createRunnable(this, function, varargs);
    }


    public void signalStart() {
        executor.onStart(this);
    }


    public void signalEnd() {
        synchronized (mutex) {
            complete = true;
            mutex.notifyAll();
        }

        executor.onFinish(this);
    }

    /**
     * Called by lua to wait for the termination of a exceution.
     *
     * It is not possible to wait for yourself as this would block forever.
     */
    public void await(long id) {
        if (id == getID()) {
            throw new LuaError("cant self await");
        }

        executor.await(id);
    }

    /**
     * Called by lua to determine if a execution is still running.
     */
    public boolean running(long id) {
        return executor.running(id);
    }


    /**
     * Called to create a new excution in the current environment with this current execution as its parent.
     * This is not called directly by lua but instead everytime a runnable/callable is called somehow.
     */
    public LuaExecution newExecution() {
        return executor.newExecution(this);
    }


    /**
     * Called to wait for the termination of this excecution. Not called directly by lua instead called from a another executions await method via the executor.
     */
    public void waitFor() {
        synchronized (mutex) {
            if (complete) {
                return;
            }

            try {
                mutex.wait();
            } catch (InterruptedException e) {
                Util.interrupted();
            }
        }
    }


}
