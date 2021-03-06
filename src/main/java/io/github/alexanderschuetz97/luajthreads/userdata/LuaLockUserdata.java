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
package io.github.alexanderschuetz97.luajthreads.userdata;

import org.luaj.vm2.LuaUserdata;
import org.luaj.vm2.LuaValue;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class LuaLockUserdata extends LuaUserdata {

    protected final Map<LuaValue, LuaValue> binding;

    public LuaLockUserdata(Map<LuaValue, LuaValue> binding) {
        super(new ReentrantLock());
        this.binding = binding;
    }

    @Override
    public LuaValue get(LuaValue key) {
        LuaValue bound = binding.get(key);
        if (bound == null) {
            return super.get(key);
        }

        return bound;
    }


}
