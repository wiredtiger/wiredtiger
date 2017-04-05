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

/*
 * workgen.swig
 *	The SWIG interface file defining the workgen python API.
 */

%include "typemaps.i"
%include "std_vector.i"
%include "std_string.i"

/* We only need to reference WiredTiger types. */
%import "wiredtiger.h"

%{
#include <ostream>
#include <sstream>
#include "wiredtiger.h"
#include "workgen.h"
%}

%pythoncode %{
import numbers
%}

%exception {
	try {
		$action
	}
	catch (workgen::WorkgenException &wge) {
		SWIG_exception_fail(SWIG_RuntimeError, wge._str.c_str());
	}
}

/*
 * Some functions are long running, turn off signal handling that was enabled
 * by the Python interpreter.  This means that a signal handler coded in Python
 * won't work when spanning a call to one of these long running functions, but
 * it's doubtful our test scripts need signals at all.  This could be made to
 * work, it's just not worth the trouble.
 */
%define InterruptableFunction(funcname)
%exception funcname {
	try {
		void (*savesig)(int) = signal(SIGINT, SIG_DFL);
		$action
		(void)signal(SIGINT, savesig);
	}
	catch (workgen::WorkgenException &wge) {
		SWIG_exception_fail(SWIG_RuntimeError, wge._str.c_str());
	}
}
%enddef

InterruptableFunction(workgen::execute)
InterruptableFunction(workgen::Workload::run)

%module workgen
/* Parse the header to generate wrappers. */
%include "workgen.h"

%template(OpList) std::vector<workgen::Operation>;
%template(ThreadList) std::vector<workgen::Thread>;

%define WorkgenClass(classname)
%extend workgen::classname {
	const std::string __str__() {
		std::ostringstream out;
		$self->describe(out);
		return out.str();
	}
};
%enddef

WorkgenClass(Key)
WorkgenClass(Operation)
WorkgenClass(Thread)
WorkgenClass(Table)
WorkgenClass(TableStats)
WorkgenClass(Value)
WorkgenClass(Workload)

%extend workgen::Operation {
%pythoncode %{
    def __mul__(self, other):
        if not isinstance(other, numbers.Integral):
            raise Exception('Operation.__mul__ requires an integral number')
        op = Operation()
        op._children = OpList([self])
        op._repeatchildren = other
        return op

    __rmul__ = __mul__

    def __add__(self, other):
        if type(self) != type(other):
            raise Exception('Operation.__sum__ requires an Operation')
        if self._children == None or self._repeatchildren != 1:
            op = Operation()
            op._children = OpList([self, other])
            op._repeatchildren = 1
            return op
        else:
            self._children.append(other)
            return self
%}
};
