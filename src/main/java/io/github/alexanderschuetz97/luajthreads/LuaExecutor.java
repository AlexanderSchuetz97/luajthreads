//
// Copyright Alexander Schütz, 2021
//
// This file is part of luajthreads.
//
// luajthreads is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// luajthreads is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// A copy of the GNU General Public License should be provided
// in the COPYING file in top level directory of luajthreads.
// If not, see <https://www.gnu.org/licenses/>.
//
package io.github.alexanderschuetz97.luajthreads;

import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This object represents a lua execution environment.
 * It has the shared values of the environment and keeps track of a counter of started and finished executions.
 * Keep in mind that a thread/task may create multiple executors with id 0 but can only be present in ONE executor
 * with a non 0 id.
 */
public class LuaExecutor {

    private final LuaThreadLib threadLib;
    private final AtomicLong counter = new AtomicLong(0);
    private final ConcurrentMap<LuaValue, LuaValue> sharedValues = new ConcurrentHashMap<>();
    private final Map<Long, LuaExecution> executors = new ConcurrentHashMap<>();

    public LuaExecutor(LuaThreadLib threadLib) {
        this.threadLib = threadLib;
    }

    public LuaThreadLib getThreadLib() {
        return threadLib;
    }

    private long nextId() {
        long l = counter.getAndIncrement();
        if (l < 0) {
            //Unrealistic.
            throw new LuaError("id overflow.");
        }

        return l;
    }

    public LuaExecution newExecution(LuaExecution parent) {
        return new LuaExecution(this, nextId(), parent == null ? -1 : parent.getID());
    }

    public void onStart(LuaExecution execution) {
        executors.put(execution.getID(), execution);
    }

    public void onFinish(LuaExecution execution) {
        executors.remove(execution.getID());
    }

    public boolean running(long id) {
        if (id == 0) {
            return true;
        }
        return executors.containsKey(id);
    }

    public void await(long id) {
        if (id == 0) {
            throw new LuaError("cant wait on main execution.");
        }
        LuaExecution exc = executors.get(id);
        if (exc == null) {
            return;
        }

        exc.waitFor();
    }

    public LuaValue putSharedValues(LuaValue aKey, LuaValue aValue) {
        Util.checkKey(aKey);
        Util.checkValue(aValue);
        LuaValue previous = sharedValues.put(aKey, aValue);
        return previous == null ? LuaValue.NIL : previous;
    }

    public LuaValue putSharedValueIfAbsent(LuaValue aKey, LuaValue aValue) {
        Util.checkKey(aKey);
        Util.checkValue(aValue);
        LuaValue previous = sharedValues.putIfAbsent(aKey, aValue);
        return previous == null ? LuaValue.NIL : previous;
    }

    public LuaValue getSharedValue(LuaValue aKey) {
        Util.checkKey(aKey);
        LuaValue value = sharedValues.get(aKey);
        return value == null ? LuaValue.NIL : value;
    }

    public LuaValue getSharedValueKeys() {
        LuaTable table = new LuaTable();
        int i = 1;
        for (LuaValue lv : sharedValues.keySet()) {
            table.set(i++, lv);
        }
        return table;
    }

    public void clearSharedValues() {
        sharedValues.clear();
    }


}
