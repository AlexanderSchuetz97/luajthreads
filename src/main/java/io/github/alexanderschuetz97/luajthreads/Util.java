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
import io.github.alexanderschuetz97.luajthreads.userdata.LuaLockUserdata;
import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaClosure;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaFunction;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Upvaldesc;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.compiler.LuaC;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.concurrent.Future;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class Util {

    public static Condition checkCondition(LuaValue arg) {
        return (Condition) arg.checkuserdata(Condition.class);
    }

    public static ReentrantLock checkLock(LuaValue arg) {
        return (ReentrantLock) arg.checkuserdata(ReentrantLock.class);
    }

    public static LuaExecution checkExecution(LuaValue arg) {
        return (LuaExecution) arg.checkuserdata(LuaExecution.class);
    }

    public static Future<?> checkFuture(LuaValue arg) {
        return (Future<?>) arg.checkuserdata(Future.class);
    }

    public static TransferQueue checkTransferQueue(LuaValue arg) {
        return (TransferQueue) arg.checkuserdata(TransferQueue.class);
    }



    /**
     * Check if value is a immutable key that has consistent equals+hashCode.
     */
    public static void checkKey(LuaValue aKey) {
        int keyType = aKey.type();
        if (keyType > 4 || keyType == 2) {
            throw new LuaError("key must be nil, boolean, string or number is " + aKey.typename());
        }
    }

    /**
     * Check if the value safe to be passed from one thread to another.
     */
    public static void checkValue(LuaValue aValue) {
        int valueType = aValue.type();
        if (valueType > 7 || valueType == 5 || valueType == 6) {
            throw new LuaError("value must be nil, boolean, userdata, string or number is " + aValue.typename());
        }

        if (aValue.isuserdata(LuaExecution.class)) {
            throw new LuaError("value must not be LuaExecution");
        }
    }

    /**
     * Check if the value safe to be passed from one thread to another.
     */
    public static Varargs checkValues(Varargs args) {
        for (int i = 1; i <= args.narg();i++) {
            checkValue(args.arg(i));
        }
        return args;
    }

    public static LuaValue interrupted() {
        throw new LuaError("interrupted");
    }

    public static LuaValue noExecutor() {
        throw new LuaError("no executor");
    }

    public static LuaValue noScheduler() {
        throw new LuaError("no scheduler");
    }

    public static Varargs checkQueueResult(Object result) {
        if (result instanceof Varargs) {
            return (Varargs) result;
        }

        if (result == null) {
            return LuaValue.NONE;
        }

        throw new LuaError("TransferQueue was corrupted externally. Contains non Varargs value " + result.getClass().getName());
    }
}
