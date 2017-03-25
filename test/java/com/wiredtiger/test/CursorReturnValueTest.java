/*-
 * Public Domain 2014-2016 MongoDB, Inc.
 * Public Domain 2008-2014 WiredTiger, Inc.
 *
 * This is free and unencumbered software released into the public domain.
 *
 * Anyone is free to copy, modify, publish, use, compile, sell, or
 * distribute this software, either in source code form or as a compiled
 * binary, for any purpose, commercial or non-commercial, and by any
 * means.
 *
 * In jurisdictions that recognize copyright laws, the author or authors
 * of this software dedicate any and all copyright interest in the
 * software to the public domain. We make this dedication for the benefit
 * of the public at large and to the detriment of our heirs and
 * successors. We intend this dedication to be an overt act of
 * relinquishment in perpetuity of all present and future rights to this
 * software under copyright law.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
package com.wiredtiger.test;

import com.wiredtiger.db.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Test cases related to Cursor methods that return integers.
 *
 * In most cases, the C language methods will utilize an int
 * return value to indicate success and any integer result
 * will be passed out via a pointer argument.
 *
 * The Swig generation logic has a type map setup that maps
 * all int return values through a bit of logic that translates
 * non-zero return values to Java exceptions.
 *
 * For Cursor::compare and Cursor::equals (more accurately,
 * compare_wrap and equals_wrap), the return value of the C-
 * language method is checked in the Swig method body and
 * the expectation is that the result of the comparison should
 * be returned verbatim without interpolating non-zero return
 * values as Exceptions.
 *
 * These tests illustrate the bug. My next commit will fix
 * the bug (and this message) in the Swig generation logic.
 * Currently, calls that would have returned 1 actually throw
 * a WiredTigerException with the POSIX error string for ERRNO
 * 1.
 */
public class CursorReturnValueTest {

    private Connection conn;
    private Session s;
    private Cursor c;
    private Cursor c2;


    @Test
    public void cursor_compare_equals()
    throws WiredTigerPackingException {

        c.putKeyString("foo");
        Assert.assertEquals(SearchStatus.FOUND, c.search_near());

        c2.putKeyString("foo");
        Assert.assertEquals(SearchStatus.FOUND, c2.search_near());

        Assert.assertEquals(0, c.compare(c2));
        Assert.assertEquals(0, c2.compare(c));
    }

    @Test
    public void cursor_compare_non_equals()
    throws WiredTigerPackingException {

        c.putKeyString("foo");
        Assert.assertEquals(SearchStatus.FOUND, c.search_near());

        c2.putKeyString("fizz");
        Assert.assertEquals(SearchStatus.FOUND, c2.search_near());

        Assert.assertEquals(1, c.compare(c2));
        Assert.assertEquals(-1, c2.compare(c));
    }


    @Test
    public void cursor_equals()
    throws WiredTigerPackingException {

        c.putKeyString("foo");
        Assert.assertEquals(SearchStatus.FOUND, c.search_near());

        c2.putKeyString("foo");
        Assert.assertEquals(SearchStatus.FOUND, c2.search_near());

        Assert.assertEquals(1, c.equals(c2));
        Assert.assertEquals(1, c2.equals(c));
    }

    @Test
    public void cursor_non_equals()
    throws WiredTigerPackingException {

        c.putKeyString("foo");
        Assert.assertEquals(SearchStatus.FOUND, c.search_near());

        c2.putKeyString("fizz");
        Assert.assertEquals(SearchStatus.FOUND, c2.search_near());

        Assert.assertEquals(0, c.equals(c2));
        Assert.assertEquals(0, c2.equals(c));
    }

    @Before
    public void setup() {
        conn = wiredtiger.open("WT_HOME", "create");
        s = conn.open_session(null);
        s.create("table:t", "key_format=S,value_format=S");

        c = s.open_cursor("table:t", null, null);
        c.putKeyString("foo");
        c.putValueString("bar");
        c.insert();
        c.reset();

        c2 = s.open_cursor("table:t", null, null);
        c.putKeyString("fizz");
        c.putValueString("fazz");
        c.insert();
        c.reset();
    }

    @After
    public void teardown() {

        c.close();
        c2.close();

        s.drop("table:t", "");
        s.close("");
        conn.close("");
    }

}

