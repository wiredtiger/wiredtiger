/*-
 * Copyright (c) 2008-2012 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 * wiredtiger.i
 *	The SWIG interface file defining the wiredtiger python API.
 */

%define DOCSTRING
"@defgroup wt_python WiredTiger Python API
Python wrappers aroung the WiredTiger C API.
@{
@cond IGNORE"
%enddef

%module(docstring=DOCSTRING) wiredtiger

%feature("autodoc", "0");

%pythoncode %{
from packing import pack, unpack
## @endcond
%}

/* Set the input argument to point to a temporary variable */ 
%typemap(in, numinputs=0) WT_CONNECTION ** (WT_CONNECTION *temp = NULL) {
	$1 = &temp;
}
%typemap(in, numinputs=0) WT_SESSION ** (WT_SESSION *temp = NULL) {
	$1 = &temp;
}
%typemap(in, numinputs=0) WT_CURSOR ** (WT_CURSOR *temp = NULL) {
	$1 = &temp;
}

/* Event handlers are not supported in Python. */
%typemap(in, numinputs=0) WT_EVENT_HANDLER * { $1 = NULL; }

/* Set the return value to the returned connection, session, or cursor */
%typemap(argout) WT_CONNECTION ** {
	$result = SWIG_NewPointerObj(SWIG_as_voidptr(*$1),
	    SWIGTYPE_p___wt_connection, 0);
}
%typemap(argout) WT_SESSION ** {
	$result = SWIG_NewPointerObj(SWIG_as_voidptr(*$1),
	    SWIGTYPE_p___wt_session, 0);
}

%typemap(argout) WT_CURSOR ** {
	$result = SWIG_NewPointerObj(SWIG_as_voidptr(*$1),
	    SWIGTYPE_p___wt_cursor, 0);
	if (*$1 != NULL) {
		(*$1)->flags |= WT_CURSTD_RAW;
		PyObject_SetAttrString($result, "is_column",
		    PyBool_FromLong(strcmp((*$1)->key_format, "r") == 0));
	}
}

/* 64 bit typemaps. */
%typemap(in) uint64_t {
	$1 = PyLong_AsUnsignedLongLong($input);
}
%typemap(out) uint64_t {
	$result = PyLong_FromUnsignedLongLong($1);
}

/* Throw away references after close. */
%define DESTRUCTOR(class, method)
%feature("shadow") class::method %{
	def method(self, *args):
		'''close(self, config) -> int
		
		@copydoc class::method'''
		try:
			return $action(self, *args)
		finally:
			self.this = None
%}
%enddef
DESTRUCTOR(__wt_connection, close)
DESTRUCTOR(__wt_cursor, close)
DESTRUCTOR(__wt_session, close)

/* Don't require empty config strings. */
%typemap(default) const char *config { $1 = NULL; }

/* 
 * Error returns other than WT_NOTFOUND generate an exception.
 * Use our own exception type, in future tailored to the kind
 * of error.
 */
%header %{
static PyObject *wtError;
%}

%init %{
	/*
	 * Create an exception type and put it into the _wiredtiger module.
	 * First increment the reference count because PyModule_AddObject
	 * decrements it.  Then note that "m" is the local variable for the
	 * module in the SWIG generated code.  If there is a SWIG variable for
	 * this, I haven't found it.
	 */
	wtError = PyErr_NewException("_wiredtiger.WiredTigerError", NULL, NULL);
	Py_INCREF(wtError);
	PyModule_AddObject(m, "WiredTigerError", wtError);
%}

%pythoncode %{
WiredTigerError = _wiredtiger.WiredTigerError

## @cond DISABLE
# Implements the iterable contract
class IterableCursor:
	def __init__(self, cursor):
		self.cursor = cursor

	def __iter__(self):
		return self

	def next(self):
		if self.cursor.next() == WT_NOTFOUND:
			raise StopIteration
		return self.cursor.get_keys() + self.cursor.get_values()
## @endcond
%}

/*
 * Extra 'self' elimination.
 * The methods we're wrapping look like this:
 * struct __wt_xxx {
 *	int method(WT_XXX *, ...otherargs...);
 * };
 * To SWIG, that is equivalent to:
 *	int method(struct __wt_xxx *self, WT_XXX *, ...otherargs...);
 * and we use consecutive argument matching of typemaps to convert two args to
 * one.
 */
%define SELFHELPER(type, name)
%typemap(in) (type *self, type *name) (void *argp = 0, int res = 0) %{
	res = SWIG_ConvertPtr($input, &argp, $descriptor, $disown | 0);
	if (!SWIG_IsOK(res)) { 
		SWIG_exception_fail(SWIG_ArgError(res), "in method '$symname', "
		    "argument $argnum of type '$type'");
	}
	$2 = $1 = ($ltype)(argp);
%}
%enddef

SELFHELPER(struct __wt_connection, connection)
SELFHELPER(struct __wt_session, session)
SELFHELPER(struct __wt_cursor, cursor)

/* WT_CURSOR customization. */
/* First, replace the varargs get / set methods with Python equivalents. */
%ignore __wt_cursor::get_key;
%ignore __wt_cursor::get_value;
%ignore __wt_cursor::set_key;
%ignore __wt_cursor::set_value;

/* Next, override methods that return integers via arguments. */
%ignore __wt_cursor::equals(WT_CURSOR *, WT_CURSOR *, int *);
%ignore __wt_cursor::search_near(WT_CURSOR *, int *);

/* SWIG magic to turn Python byte strings into data / size. */
%apply (char *STRING, int LENGTH) { (char *data, int size) };

%extend __wt_cursor {
	/* Get / set keys and values */
	void _set_key(char *data, int size) {
		WT_ITEM k;
		k.data = data;
		k.size = (uint32_t)size;
		$self->set_key($self, &k);
	}

	void _set_recno(uint64_t recno) {
		WT_ITEM k;
		uint8_t recno_buf[20];
		size_t size;
		int ret = wiredtiger_struct_pack($self->session,
		    recno_buf, sizeof (recno_buf), "r", recno);
		if (ret == 0)
			ret = wiredtiger_struct_size($self->session,
			    &size, "q", recno);
		if (ret != 0) {
			SWIG_Python_SetErrorMsg(wtError,
			    wiredtiger_strerror(ret));
			return;
		}
		k.data = recno_buf;
		k.size = (uint32_t)size;
		$self->set_key($self, &k);
	}

	void _set_value(char *data, int size) {
		WT_ITEM v;
		v.data = data;
		v.size = (uint32_t)size;
		$self->set_value($self, &v);
	}

	PyObject *_get_key() {
		WT_ITEM k;
		int ret = $self->get_key($self, &k);
		if (ret != 0) {
			SWIG_Python_SetErrorMsg(wtError,
			    wiredtiger_strerror(ret));
			return (NULL);
		}
		return SWIG_FromCharPtrAndSize(k.data, k.size);
	}

	PyObject *_get_recno() {
		WT_ITEM k;
		uint64_t r;
		int ret = $self->get_key($self, &k);
		if (ret == 0)
			ret = wiredtiger_struct_unpack($self->session,
			    k.data, k.size, "q", &r);
		if (ret != 0) {
			SWIG_Python_SetErrorMsg(wtError,
			    wiredtiger_strerror(ret));
			return (NULL);
		}
		return PyLong_FromUnsignedLongLong(r);
	}

	PyObject *_get_value() {
		WT_ITEM v;
		int ret = $self->get_value($self, &v);
		if (ret != 0) {
			SWIG_Python_SetErrorMsg(wtError,
			    wiredtiger_strerror(ret));
			return (NULL);
		}
		return SWIG_FromCharPtrAndSize(v.data, v.size);
	}

	/* equals and search_near need special handling. */
	PyObject *equals(WT_CURSOR *other) {
		int is_equal = 0;
		int ret = $self->equals($self, other, &is_equal);
		if (ret != 0) {
			SWIG_Python_SetErrorMsg(wtError,
			    wiredtiger_strerror(ret));
			return (NULL);
		}
		return (SWIG_From_int(is_equal));
	}

	PyObject *search_near() {
		int cmp = 0;
		int ret = $self->search_near($self, &cmp);
		if (ret != 0 && ret != WT_NOTFOUND) {
			SWIG_Python_SetErrorMsg(wtError,
			    wiredtiger_strerror(ret));
			return (NULL);
		}
		/*
		 * Map less-than-zero to -1 and greater-than-zero to 1 to avoid
		 * colliding with WT_NOTFOUND.
		 */
		return (SWIG_From_int((ret != 0) ? ret :
		    (cmp < 0) ? -1 : (cmp == 0) ? 0 : 1));
	}

%pythoncode %{
	def get_key(self):
		'''get_key(self) -> object
		
		@copydoc WT_CURSOR::get_key
		Returns only the first column.'''
		return self.get_keys()[0]

	def get_keys(self):
		'''get_keys(self) -> (object, ...)
		
		@copydoc WT_CURSOR::get_key'''
		if self.is_column:
			return [self._get_recno(),]
		else:
			return unpack(self.key_format, self._get_key())

	def get_value(self):
		'''get_value(self) -> object
		
		@copydoc WT_CURSOR::get_value
		Returns only the first column.'''
		return self.get_values()[0]

	def get_values(self):
		'''get_values(self) -> (object, ...)
		
		@copydoc WT_CURSOR::get_value'''
		return unpack(self.value_format, self._get_value())

	def set_key(self, *args):
		'''set_key(self) -> None
		
		@copydoc WT_CURSOR::set_key'''
		if self.is_column:
			self._set_recno(long(args[0]))
		else:
			# Keep the Python string pinned
			self._key = pack(self.key_format, *args)
			self._set_key(self._key)

	def set_value(self, *args):
		'''set_value(self) -> None
		
		@copydoc WT_CURSOR::set_value'''
		# Keep the Python string pinned
		self._value = pack(self.value_format, *args)
		self._set_value(self._value)

	def __iter__(self):
		'''Cursor objects support iteration, equivalent to calling
		WT_CURSOR::next until it returns ::WT_NOTFOUND.'''
		if not hasattr(self, '_iterable'):
			self._iterable = IterableCursor(self)
		return self._iterable
%}
};

/* Remove / rename parts of the C API that we don't want in Python. */
%immutable __wt_cursor::session;
%immutable __wt_cursor::uri;
%immutable __wt_cursor::key_format;
%immutable __wt_cursor::value_format;
%immutable __wt_session::connection;

%ignore __wt_buf;
%ignore __wt_collator;
%ignore __wt_connection::add_collator;
%ignore __wt_compressor;
%ignore __wt_connection::add_compressor;
%ignore __wt_data_source;
%ignore __wt_connection::add_data_source;
%ignore __wt_event_handler;
%ignore __wt_extractor;
%ignore __wt_connection::add_extractor;
%ignore __wt_item;

%ignore wiredtiger_struct_pack;
%ignore wiredtiger_struct_packv;
%ignore wiredtiger_struct_size;
%ignore wiredtiger_struct_sizev;
%ignore wiredtiger_struct_unpack;
%ignore wiredtiger_struct_unpackv;

%ignore wiredtiger_extension_init;

/*
 * Error handling.  This comes last so it doesn't interfere with the extension
 * code above.
 * 
 * Default case: a non-zero return is an error.
 */
%exception {
	$action
	if (result != 0) {
		/* We could use PyErr_SetObject for more complex reporting. */
		SWIG_Python_SetErrorMsg(wtError, wiredtiger_strerror(result));
		SWIG_fail;
	}
}

/* Cursor positioning methods can also return WT_NOTFOUND. */
%define NOTFOUND_OK(m)
%exception m {
	$action
	if (result != 0 && result != WT_NOTFOUND) {
		/* We could use PyErr_SetObject for more complex reporting. */
		SWIG_Python_SetErrorMsg(wtError, wiredtiger_strerror(result));
		SWIG_fail;
	}
}
%enddef

NOTFOUND_OK(__wt_cursor::next)
NOTFOUND_OK(__wt_cursor::prev)
NOTFOUND_OK(__wt_cursor::remove)
NOTFOUND_OK(__wt_cursor::search)
NOTFOUND_OK(__wt_cursor::update)

/* Lastly, some methods need no (additional) error checking. */
%exception __wt_connection::equals;
%exception __wt_connection::search_near;
%exception __wt_connection::get_home;
%exception __wt_connection::is_new;
%exception wiredtiger_strerror;
%exception wiredtiger_version;

/* Convert 'int *' to output args for wiredtiger_version */
%apply int *OUTPUT { int * };

%rename(Cursor) __wt_cursor;
%rename(Session) __wt_session;
%rename(Connection) __wt_connection;

%include "wiredtiger.h"

%pythoncode %{
## @}

class stat:
	""" a set of static defines used by statistics cursor """
	pass

class filestat:
	""" a set of static defines used by statistics cursor """
	pass

import sys
# All names starting with 'WT_STAT_file_' are renamed to
# the wiredtiger.filestat class, those starting with 'WT_STAT_' are
# renamed to wiredtiger.stat .
def _rename_with_prefix(prefix, toclass):
	curmodule = sys.modules[__name__]
	for name in dir(curmodule):
		if name.startswith(prefix):
			shortname = name[len(prefix):]
			setattr(toclass, shortname, getattr(curmodule, name))
			delattr(curmodule, name)

_rename_with_prefix('WT_STAT_file_', filestat)
_rename_with_prefix('WT_STAT_', stat)
del _rename_with_prefix
%}

