package io.github.alexanderschuetz97.luajthreads;

import org.junit.Assert;
import org.junit.Test;
import org.luaj.vm2.Globals;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;
import org.luaj.vm2.compiler.LuaC;
import org.luaj.vm2.lib.jse.JsePlatform;
import org.luaj.vm2.luajc.LuaJC;

public class LuaThreadLibTest {



    private static Globals globalsJC() {
        Globals gl = JsePlatform.standardGlobals();
        LuaC.install(gl);
        LuaJC.install(gl);
        gl.load(new LuaThreadLib());
        return gl;
    }


    private static Globals globals() {
        Globals gl = JsePlatform.standardGlobals();
        LuaC.install(gl);
        gl.load(new LuaThreadLib());
        return gl;
    }

    private static Varargs testScript(String scriptname) {
        Globals gl = globals();
        return gl.load(LuaThreadLibTest.class.getResourceAsStream("/" + scriptname), scriptname, "bt", gl).invoke();
    }

    private static Varargs testScriptJC(String scriptname) {
        Globals gl = globalsJC();
        return gl.load(LuaThreadLibTest.class.getResourceAsStream("/" + scriptname), scriptname, "bt", gl).invoke();
    }

    @Test
    public void testSubmit() {
        Assert.assertEquals("1 0 Hello World", testScript("testsubmit.lua").checkjstring(2));
        Assert.assertEquals("1 0 Hello World", testScriptJC("testsubmit.lua").checkjstring(2));
    }

    @Test
    public void testUpValues() {
        testScript("testupvalues.lua");
        testScriptJC("testupvalues.lua");
    }

    @Test
    public void testUtil() {
        testScript("testutil.lua");
        testScriptJC("testutil.lua");
    }

    @Test
    public void testLoadFile() {
        Assert.assertTrue(testScript("loadfiletest.lua").checkboolean(1));
        Assert.assertTrue(testScriptJC("loadfiletest.lua").checkboolean(1));
    }
}
