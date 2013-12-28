/*-
 * Copyright (c) 2008-2013 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 *
 * wiredtiger.i
 *	The SWIG interface file defining the wiredtiger Java API.
 */

%module wiredtiger

%include "enums.swg"
%include "typemaps.i"

%pragma(java) jniclasscode=%{
  static {
    try {
	System.loadLibrary("wiredtiger_java");
    } catch (UnsatisfiedLinkError e) {
      System.err.println("Native code library failed to load. \n" + e);
      System.exit(1);
    }
  }
%}

%{
#include "../src/include/wt_internal.h"

/*
 * Closed handle checking:
 *
 * The typedef WT_CURSOR_NULLABLE used in wiredtiger.h is only made
 * visible to the SWIG parser and is used to identify arguments of
 * Cursor type that are permitted to be null.  Likewise, typedefs
 * WT_{CURSOR,SESSION,CONNECTION}_CLOSED identify 'close' calls that
 * need explicit nulling of the swigCPtr.  These typedefs permit
 * special casing in typemaps for input args.
 *
 * We want SWIG to see these 'fake' typenames, but not the compiler.
 */
#define WT_CURSOR_NULLABLE		WT_CURSOR
#define WT_CURSOR_CLOSED		WT_CURSOR
#define WT_SESSION_CLOSED		WT_SESSION
#define WT_CONNECTION_CLOSED		WT_CONNECTION

/*
 * For Connections, Sessions and Cursors created in Java, each of
 * WT_CONNECTION_IMPL, WT_SESSION_IMPL and WT_CURSOR have a
 * lang_private field that store a pointer to a JAVA_CALLBACK, alloced
 * during the various open calls.  {conn,session,cursor}CloseHandler()
 * functions reach into the associated java object, set the swigCPtr
 * to 0, and free the JAVA_CALLBACK. Typemaps matching Connection,
 * Session, Cursor args use the NULL_CHECK macro, which checks if
 * swigCPtr is 0.
 */
typedef struct {
	JNIEnv *jnienv;		/* jni env that created the Session/Cursor */
	jobject jobj;		/* the java Session/Cursor object */
	jfieldID fid;		/* cached Cursor.swigCPtr field id in session */
} JAVA_CALLBACK;

static void throwWiredTigerException(JNIEnv *jenv, const char *msg) {
	jclass excep = (*jenv)->FindClass(jenv, "com/wiredtiger/db/WiredTigerException");
	if (excep)
		(*jenv)->ThrowNew(jenv, excep, msg);
}

%}

/* No finalizers */
%typemap(javafinalize) SWIGTYPE ""

/* Event handlers are not supported in Java. */
%typemap(in, numinputs=0) WT_EVENT_HANDLER * %{ $1 = NULL; %}

/* Allow silently passing the Java object and JNIEnv into our code. */
%typemap(in, numinputs=0) jobject *jthis %{ $1 = jarg1_; %}
%typemap(in, numinputs=0) JNIEnv * %{ $1 = jenv; %}

/* 64 bit typemaps. */
%typemap(jni) uint64_t "jlong"
%typemap(jtype) uint64_t "long"
%typemap(jstype) uint64_t "long"

%typemap(javain) uint64_t "$javainput"
%typemap(javaout) uint64_t {
	return $jnicall;
}

/* Return byte[] from cursor.get_value */
%typemap(jni) WT_ITEM, WT_ITEM * "jbyteArray"
%typemap(jtype) WT_ITEM, WT_ITEM * "byte[]"
%typemap(jstype) WT_ITEM, WT_ITEM * "byte[]"

%typemap(javain) WT_ITEM, WT_ITEM * "$javainput"
%typemap(javaout) WT_ITEM, WT_ITEM * {
	return $jnicall;
}

%typemap(in) WT_ITEM * (WT_ITEM item) %{
	$1 = &item;
	$1->data = (*jenv)->GetByteArrayElements(jenv, $input, 0);
	$1->size = (*jenv)->GetArrayLength(jenv, $input);
%}

%typemap(argout) WT_ITEM * %{
	(*jenv)->ReleaseByteArrayElements(jenv, $input, (void *)$1->data, 0);
%}

%typemap(out) WT_ITEM %{
	if ($1.data == NULL)
		$result = NULL;
	else if (($result = (*jenv)->NewByteArray(jenv, $1.size)) != NULL) {
		(*jenv)->SetByteArrayRegion(jenv,
		    $result, 0, $1.size, $1.data);
	}
%}

/* Don't require empty config strings. */
%typemap(default) const char *config %{ $1 = NULL; %}

%typemap(out) int %{
	if ($1 != 0 && $1 != WT_NOTFOUND) {
		throwWiredTigerException(jenv, wiredtiger_strerror($1));
		return $null;
	}
	$result = $1;
%}

%define NULL_CHECK(val, name)
	if (!val) { 
     		SWIG_JavaThrowException(jenv, SWIG_JavaNullPointerException, 
		#name " is null"); 
		return 0; 
	}
%enddef

%define WT_CLASS(type, class, name, closeHandler)
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
%typemap(in, numinputs=0) type *name {
	$1 = *(type **)&jarg1;
	NULL_CHECK($1, $1_name)
}

%typemap(in, numinputs=0) class ## _CLOSED *name {
	$1 = *(type **)&jarg1;
	NULL_CHECK($1, $1_name)
	closeHandler;
}

%typemap(in) class ## _NULLABLE * {
	$1 = *(type **)&$input;
}

%typemap(in) type * {
	$1 = *(type **)&$input;
	NULL_CHECK($1, $1_name)
}

%typemap(javaimports) type "
/**
  * @copydoc class
  * @ingroup wt_java
  */"
%enddef

%pragma(java) moduleimports=%{
/**
 * @defgroup wt_java WiredTiger Java API
 *
 * Java wrappers around the WiredTiger C API.
 */

/**
 * @ingroup wt_java
 */
%}

WT_CLASS(struct __wt_connection, WT_CONNECTION, connection, connCloseHandler($1))
WT_CLASS(struct __wt_session, WT_SESSION, session, sessionCloseHandler($1))
WT_CLASS(struct __wt_cursor, WT_CURSOR, cursor, cursorCloseHandler($1))

%define COPYDOC(SIGNATURE_CLASS, CLASS, METHOD)
%javamethodmodifiers SIGNATURE_CLASS::METHOD "
  /**
   * @copydoc CLASS::METHOD
   */
  public ";
%enddef

%include "java_doc.i"

/* WT_CURSOR customization. */
/* First, replace the varargs get / set methods with Java equivalents. */
%ignore __wt_cursor::get_key;
%ignore __wt_cursor::get_value;
%ignore __wt_cursor::set_key;
%ignore __wt_cursor::set_value;
%ignore __wt_cursor::insert;
%ignore __wt_cursor::remove;
%ignore __wt_cursor::search;
%ignore __wt_cursor::search_near;
%ignore __wt_cursor::update;
%javamethodmodifiers __wt_cursor::next "protected";
%rename (next_wrap) __wt_cursor::next;
%javamethodmodifiers __wt_cursor::prev "protected";
%rename (prev_wrap) __wt_cursor::prev;
%javamethodmodifiers __wt_cursor::key_format "protected";
%javamethodmodifiers __wt_cursor::value_format "protected";

%ignore __wt_cursor::compare(WT_CURSOR *, WT_CURSOR *, int *);
%rename (compare_wrap) __wt_cursor::compare;

/* SWIG magic to turn Java byte strings into data / size. */
%apply (char *STRING, int LENGTH) { (char *data, int size) };

/* Status from search_near */
%javaconst(1);
%inline %{
enum SearchStatus { FOUND, NOTFOUND, SMALLER, LARGER };
%}

%wrapper %{
/* Zero out SWIG's pointer to the C object,
 * equivalent to 'jobj.swigCPtr = 0;' in java.
 */
static int
javaClose(JAVA_CALLBACK *jcb, jfieldID *pfid)
{
	jclass cls;
	jfieldID fid;
	JNIEnv *env;

	env = jcb->jnienv;

	if (pfid == NULL || *pfid == NULL) {
		cls = (*env)->GetObjectClass(env, jcb->jobj);
		fid = (*env)->GetFieldID(env, cls, "swigCPtr", "J");
		if (pfid != NULL)
			*pfid = fid;
	} else {
		fid = *pfid;
	}
	(*env)->SetLongField(env, jcb->jobj, fid, 0L);
	(*env)->DeleteGlobalRef(env, jcb->jobj);
	return (0);
}

/* Connection specific close handler. */
static int
connCloseHandler(WT_CONNECTION *conn_arg)
{
	int ret;
	JAVA_CALLBACK *jcb;
	WT_CONNECTION_IMPL *conn;

	conn = (WT_CONNECTION_IMPL *)conn_arg;
	jcb = (JAVA_CALLBACK *)conn->lang_private;
	conn->lang_private = NULL;
	ret = javaClose(jcb, NULL);
	__wt_free(conn->default_session, jcb);

	return (0);
}

/* Session specific close handler. */
static int
sessionCloseHandler(WT_SESSION *session_arg)
{
	int ret;
	JAVA_CALLBACK *jcb;
	WT_SESSION_IMPL *session;

	session = (WT_SESSION_IMPL *)session_arg;
	jcb = (JAVA_CALLBACK *)session->lang_private;
	session->lang_private = NULL;
	ret = javaClose(jcb, NULL);
	__wt_free(session, jcb);

	return (ret);
}

/* Cursor specific close handler. */
static int
cursorCloseHandler(WT_CURSOR *cursor)
{
	int ret;
	JAVA_CALLBACK *jcb;
	JAVA_CALLBACK *sess_jcb;

	jcb = (JAVA_CALLBACK *)cursor->lang_private;
	sess_jcb = (JAVA_CALLBACK *)
	    ((WT_SESSION_IMPL *)cursor->session)->lang_private;
	cursor->lang_private = NULL;
	ret = javaClose(jcb, sess_jcb ? &sess_jcb->fid : NULL);
	__wt_free((WT_SESSION_IMPL *)cursor->session, jcb);

	return (ret);
}

/* Add event handler support. */
static int
javaCloseHandler(WT_EVENT_HANDLER *handler, WT_SESSION *session,
	WT_CURSOR *cursor)
{
	int ret;

	WT_UNUSED(handler);

	if (cursor != NULL)
		ret = cursorCloseHandler(cursor);
	else
		ret = sessionCloseHandler(session);
	return (ret);
}

WT_EVENT_HANDLER javaApiEventHandler = {NULL, NULL, NULL, javaCloseHandler};
%}

%extend __wt_cursor {

	%javamethodmodifiers get_key_wrap "protected";
	WT_ITEM get_key_wrap(JNIEnv *jenv) {
		WT_ITEM k;
		int ret;
		k.data = NULL;
		if ((ret = $self->get_key($self, &k)) != 0)
			throwWiredTigerException(jenv, wiredtiger_strerror(ret));
		return k;
	}

	%javamethodmodifiers get_value_wrap "protected";
	WT_ITEM get_value_wrap(JNIEnv *jenv) {
		WT_ITEM v;
		int ret;
		v.data = NULL;
		if ((ret = $self->get_value($self, &v)) != 0)
			throwWiredTigerException(jenv, wiredtiger_strerror(ret));
		return v;
	}

	%javamethodmodifiers insert_wrap "protected";
	int insert_wrap(WT_ITEM *k, WT_ITEM *v) {
		$self->set_key($self, k);
		$self->set_value($self, v);
		return $self->insert($self);
	}

	%javamethodmodifiers remove_wrap "protected";
	int remove_wrap(WT_ITEM *k) {
		$self->set_key($self, k);
		return $self->remove($self);
	}

	%javamethodmodifiers search_wrap "protected";
	int search_wrap(WT_ITEM *k) {
		$self->set_key($self, k);
		return $self->search($self);
	}

	%javamethodmodifiers search_near_wrap "protected";
	enum SearchStatus search_near_wrap(JNIEnv *jenv, WT_ITEM *k) {
		int cmp, ret;

		$self->set_key($self, k);
		ret = $self->search_near(self, &cmp);
		if (ret != 0 && ret != WT_NOTFOUND)
			throwWiredTigerException(jenv, wiredtiger_strerror(ret));
		if (ret == 0)
			return (cmp == 0 ? FOUND : cmp < 0 ? SMALLER : LARGER);
		return (NOTFOUND);
	}

	%javamethodmodifiers update_wrap "protected";
	int update_wrap(WT_ITEM *k, WT_ITEM *v) {
		$self->set_key($self, k);
		$self->set_value($self, v);
		return $self->update($self);
	}

	int compare_wrap(JNIEnv *jenv, WT_CURSOR *other) {
		int cmp, ret = $self->compare($self, other, &cmp);
		if (ret != 0)
			throwWiredTigerException(jenv, wiredtiger_strerror(ret));
		return cmp;
	}

	%javamethodmodifiers java_init "protected";
	int java_init(jobject jcursor) {
		JAVA_CALLBACK *jcb = (JAVA_CALLBACK *)$self->lang_private;
		jcb->jobj = JCALL1(NewGlobalRef, jcb->jnienv, jcursor);
		JCALL1(DeleteLocalRef, jcb->jnienv, jcursor);
		return (0);
	}
}

/* Cache key/value formats in Cursor */
%typemap(javabody) struct __wt_cursor %{
 private long swigCPtr;
 protected boolean swigCMemOwn;
 protected String keyFormat;
 protected String valueFormat;
 protected PackOutputStream keyPacker;
 protected PackOutputStream valuePacker;
 protected PackInputStream keyUnpacker;
 protected PackInputStream valueUnpacker;

 protected $javaclassname(long cPtr, boolean cMemoryOwn) {
   swigCMemOwn = cMemoryOwn;
   swigCPtr = cPtr;
   keyFormat = getKey_format();
   valueFormat = getValue_format();
   keyPacker = new PackOutputStream(keyFormat);
   valuePacker = new PackOutputStream(valueFormat);
   wiredtigerJNI.Cursor_java_init(swigCPtr, this, this);
 }

 protected static long getCPtr($javaclassname obj) {
   return (obj == null) ? 0 : obj.swigCPtr;
 }
%}

%typemap(javacode) struct __wt_cursor %{

	/**
	 * Retrieve the format string for this cursor's key.
	 */
	public String getKeyFormat() {
		return keyFormat;
	}

	/**
	 * Retrieve the format string for this cursor's value.
	 */
	public String getValueFormat() {
		return valueFormat;
	}

	/**
	 * Append a byte to the cursor's key.
	 *
	 * \param value The value to append.
	 * \return This cursor object, so put calls can be chained.
	 */
	public Cursor putKeyByte(byte value)
	throws WiredTigerPackingException {
		keyPacker.addByte(value);
		return this;
	}

	/**
	 * Append a byte array to the cursor's key.
	 *
	 * \param value The value to append.
	 * \return This cursor object, so put calls can be chained.
	 */
	public Cursor putKeyByteArray(byte[] value)
	throws WiredTigerPackingException {
		this.putKeyByteArray(value, 0, value.length);
		return this;
	}

	/**
	 * Append a byte array to the cursor's key.
	 *
	 * \param value The value to append.
	 * \param off The offset into value at which to start.
	 * \param len The length of the byte array.
	 * \return This cursor object, so put calls can be chained.
	 */
	public Cursor putKeyByteArray(byte[] value, int off, int len)
	throws WiredTigerPackingException {
		keyPacker.addByteArray(value, off, len);
		return this;
	}

	/**
	 * Append an integer to the cursor's key.
	 *
	 * \param value The value to append
	 * \return This cursor object, so put calls can be chained.
	 */
	public Cursor putKeyInt(int value)
	throws WiredTigerPackingException {
		keyPacker.addInt(value);
		return this;
	}

	/**
	 * Append a long to the cursor's key.
	 *
	 * \param value The value to append
	 * \return This cursor object, so put calls can be chained.
	 */
	public Cursor putKeyLong(long value)
	throws WiredTigerPackingException {
		keyPacker.addLong(value);
		return this;
	}

	/**
	 * Append a short integer to the cursor's key.
	 *
	 * \param value The value to append
	 * \return This cursor object, so put calls can be chained.
	 */
	public Cursor putKeyShort(short value)
	throws WiredTigerPackingException {
		keyPacker.addShort(value);
		return this;
	}

	/**
	 * Append a string to the cursor's key.
	 *
	 * \param value The value to append
	 * \return This cursor object, so put calls can be chained.
	 */
	public Cursor putKeyString(String value)
	throws WiredTigerPackingException {
		keyPacker.addString(value);
		return this;
	}

	/**
	 * Append a byte to the cursor's value.
	 *
	 * \param value The value to append
	 * \return This cursor object, so put calls can be chained.
	 */
	public Cursor putValueByte(byte value)
	throws WiredTigerPackingException {
		valuePacker.addByte(value);
		return this;
	}

	/**
	 * Append a byte array to the cursor's value.
	 *
	 * \param value The value to append
	 * \return This cursor object, so put calls can be chained.
	 */
	public Cursor putValueByteArray(byte[] value)
	throws WiredTigerPackingException {
		this.putValueByteArray(value, 0, value.length);
		return this;
	}

	/**
	 * Append a byte array to the cursor's value.
	 *
	 * \param value The value to append
	 * \param off The offset into value at which to start.
	 * \param len The length of the byte array.
	 * \return This cursor object, so put calls can be chained.
	 */
	public Cursor putValueByteArray(byte[] value, int off, int len)
	throws WiredTigerPackingException {
		valuePacker.addByteArray(value, off, len);
		return this;
	}

	/**
	 * Append an integer to the cursor's value.
	 *
	 * \param value The value to append
	 * \return This cursor object, so put calls can be chained.
	 */
	public Cursor putValueInt(int value)
	throws WiredTigerPackingException {
		valuePacker.addInt(value);
		return this;
	}

	/**
	 * Append a long to the cursor's value.
	 *
	 * \param value The value to append
	 * \return This cursor object, so put calls can be chained.
	 */
	public Cursor putValueLong(long value)
	throws WiredTigerPackingException {
		valuePacker.addLong(value);
		return this;
	}

	/**
	 * Append a short integer to the cursor's value.
	 *
	 * \param value The value to append
	 * \return This cursor object, so put calls can be chained.
	 */
	public Cursor putValueShort(short value)
	throws WiredTigerPackingException {
		valuePacker.addShort(value);
		return this;
	}

	/**
	 * Append a string to the cursor's value.
	 *
	 * \param value The value to append
	 * \return This cursor object, so put calls can be chained.
	 */
	public Cursor putValueString(String value)
	throws WiredTigerPackingException {
		valuePacker.addString(value);
		return this;
	}

	/**
	 * Retrieve a byte from the cursor's key.
	 *
	 * \return The requested value.
	 */
	public byte getKeyByte()
	throws WiredTigerPackingException {
		return keyUnpacker.getByte();
	}

	/**
	 * Retrieve a byte array from the cursor's key.
	 *
	 * \param output The byte array where the returned value will be stored.
	 *	       The array should be large enough to store the entire
	 *	       data item, if not a truncated value will be returned.
	 */
	public void getKeyByteArray(byte[] output)
	throws WiredTigerPackingException {
		this.getKeyByteArray(output, 0, output.length);
	}

	/**
	 * Retrieve a byte array from the cursor's key.
	 *
	 * \param output The byte array where the returned value will be stored.
	 * \param off Offset into the destination buffer to start copying into.
	 * \param len The length should be large enough to store the entire
	 *	      data item, if not a truncated value will be returned.
	 */
	public void getKeyByteArray(byte[] output, int off, int len)
	throws WiredTigerPackingException {
		keyUnpacker.getByteArray(output, off, len);
	}

	/**
	 * Retrieve a byte array from the cursor's key.
	 *
	 * \return The requested value.
	 */
	public byte[] getKeyByteArray()
	throws WiredTigerPackingException {
		return keyUnpacker.getByteArray();
	}

	/**
	 * Retrieve an integer from the cursor's key.
	 *
	 * \return The requested value.
	 */
	public int getKeyInt()
	throws WiredTigerPackingException {
		return keyUnpacker.getInt();
	}

	/**
	 * Retrieve a long from the cursor's key.
	 *
	 * \return The requested value.
	 */
	public long getKeyLong()
	throws WiredTigerPackingException {
		return keyUnpacker.getLong();
	}

	/**
	 * Retrieve a short integer from the cursor's key.
	 *
	 * \return The requested value.
	 */
	public short getKeyShort()
	throws WiredTigerPackingException {
		return keyUnpacker.getShort();
	}

	/**
	 * Retrieve a string from the cursor's key.
	 *
	 * \return The requested value.
	 */
	public String getKeyString()
	throws WiredTigerPackingException {
		return keyUnpacker.getString();
	}

	/**
	 * Retrieve a byte from the cursor's value.
	 *
	 * \return The requested value.
	 */
	public byte getValueByte()
	throws WiredTigerPackingException {
		return valueUnpacker.getByte();
	}

	/**
	 * Retrieve a byte array from the cursor's value.
	 *
	 * \param output The byte array where the returned value will be stored.
	 *	       The array should be large enough to store the entire
	 *	       data item, if not a truncated value will be returned.
	 */
	public void getValueByteArray(byte[] output)
	throws WiredTigerPackingException {
		this.getValueByteArray(output, 0, output.length);
	}

	/**
	 * Retrieve a byte array from the cursor's value.
	 *
	 * \param output The byte array where the returned value will be stored.
	 * \param off Offset into the destination buffer to start copying into.
	 * \param len The length should be large enough to store the entire
	 *	      data item, if not a truncated value will be returned.
	 */
	public void getValueByteArray(byte[] output, int off, int len)
	throws WiredTigerPackingException {
		valueUnpacker.getByteArray(output, off, len);
	}

	/**
	 * Retrieve a byte array from the cursor's value.
	 *
	 * \return The requested value.
	 */
	public byte[] getValueByteArray()
	throws WiredTigerPackingException {
		return valueUnpacker.getByteArray();
	}

	/**
	 * Retrieve an integer from the cursor's value.
	 *
	 * \return The requested value.
	 */
	public int getValueInt()
	throws WiredTigerPackingException {
		return valueUnpacker.getInt();
	}

	/**
	 * Retrieve a long from the cursor's value.
	 *
	 * \return The requested value.
	 */
	public long getValueLong()
	throws WiredTigerPackingException {
		return valueUnpacker.getLong();
	}

	/**
	 * Retrieve a short integer from the cursor's value.
	 *
	 * \return The requested value.
	 */
	public short getValueShort()
	throws WiredTigerPackingException {
		return valueUnpacker.getShort();
	}

	/**
	 * Retrieve a string from the cursor's value.
	 *
	 * \return The requested value.
	 */
	public String getValueString()
	throws WiredTigerPackingException {
		return valueUnpacker.getString();
	}

	/**
	 * Insert the cursor's current key/value into the table.
	 *
	 * \return The status of the operation.
	 */
	public int insert() {
		byte[] key = keyPacker.getValue();
		byte[] value = valuePacker.getValue();
		keyPacker.reset();
		valuePacker.reset();
		return insert_wrap(key, value);
	}

	/**
	 * Update the cursor's current key/value into the table.
	 *
	 * \return The status of the operation.
	 */
	public int update() {
		byte[] key = keyPacker.getValue();
		byte[] value = valuePacker.getValue();
		keyPacker.reset();
		valuePacker.reset();
		return update_wrap(key, value);
	}

	/**
	 * Remove the cursor's current key/value into the table.
	 *
	 * \return The status of the operation.
	 */
	public int remove() {
		byte[] key = keyPacker.getValue();
		keyPacker.reset();
		return remove_wrap(key);
	}

	/**
	 * Compare this cursor's position to another Cursor.
	 *
	 * \return The result of the comparison.
	 */
	public int compare(Cursor other) {
		return compare_wrap(other);
	}

	/**
	 * Retrieve the next item in the table.
	 *
	 * \return The result of the comparison.
	 */
	public int next() {
		int ret = next_wrap();
		keyPacker.reset();
		valuePacker.reset();
		keyUnpacker = (ret == 0) ?
		    new PackInputStream(keyFormat, get_key_wrap()) : null;
		valueUnpacker = (ret == 0) ?
		    new PackInputStream(valueFormat, get_value_wrap()) : null;
		return ret;
	}

	/**
	 * Retrieve the previous item in the table.
	 *
	 * \return The result of the comparison.
	 */
	public int prev() {
		int ret = prev_wrap();
		keyPacker.reset();
		valuePacker.reset();
		keyUnpacker = (ret == 0) ?
		    new PackInputStream(keyFormat, get_key_wrap()) : null;
		valueUnpacker = (ret == 0) ?
		    new PackInputStream(valueFormat, get_value_wrap()) : null;
		return ret;
	}

	/**
	 * Search for an item in the table.
	 *
	 * \return The result of the comparison.
	 */
	public int search() {
		int ret = search_wrap(keyPacker.getValue());
		keyPacker.reset();
		valuePacker.reset();
		keyUnpacker = (ret == 0) ?
		    new PackInputStream(keyFormat, get_key_wrap()) : null;
		valueUnpacker = (ret == 0) ?
		    new PackInputStream(valueFormat, get_value_wrap()) : null;
		return ret;
	}

	/**
	 * Search for an item in the table.
	 *
	 * \return The result of the comparison.
	 */
	public SearchStatus search_near() {
		SearchStatus ret = search_near_wrap(keyPacker.getValue());
		keyPacker.reset();
		valuePacker.reset();
		keyUnpacker = (ret != SearchStatus.NOTFOUND) ?
		    new PackInputStream(keyFormat, get_key_wrap()) : null;
		valueUnpacker = (ret != SearchStatus.NOTFOUND) ?
		    new PackInputStream(valueFormat, get_value_wrap()) : null;
		return ret;
	}
%}

/* Remove / rename parts of the C API that we don't want in Java. */
%immutable __wt_cursor::session;
%immutable __wt_cursor::uri;
%immutable __wt_cursor::key_format;
%immutable __wt_cursor::value_format;
%immutable __wt_session::connection;

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
%ignore __wt_session::msg_printf;

%ignore wiredtiger_struct_pack;
%ignore wiredtiger_struct_size;
%ignore wiredtiger_struct_unpack;

%ignore wiredtiger_version;

%ignore __wt_connection::get_extension_api;
%ignore wiredtiger_extension_init;
%ignore wiredtiger_extension_terminate;

%ignore wiredtiger_open;
%javamethodmodifiers wiredtiger_open_wrap "
  /**
   * @copydoc ::wiredtiger_open
   */
  public ";

%rename(open) wiredtiger_open_wrap;
%ignore __wt_connection::open_session;
%rename(open_session) __wt_connection::open_session_wrap;
%ignore __wt_session::open_cursor;
%javamethodmodifiers __wt_session::open_cursor_wrap "
  /**
   * @copydoc WT_SESSION::open_cursor
   */
  public ";
%rename(open_cursor) __wt_session::open_cursor_wrap;

%rename(Cursor) __wt_cursor;
%rename(Session) __wt_session;
%rename(Connection) __wt_connection;

%define TRACKED_CLASS(jclassname, ctypename, java_init_fcn, implclass)
%ignore jclassname::jclassname();

%typemap(javabody) struct ctypename %{
 private long swigCPtr;
 protected boolean swigCMemOwn;

 protected $javaclassname(long cPtr, boolean cMemoryOwn) {
   swigCMemOwn = cMemoryOwn;
   swigCPtr = cPtr;
   java_init_fcn(swigCPtr, this, this);
 }

 protected static long getCPtr($javaclassname obj) {
   return (obj == null) ? 0 : obj.swigCPtr;
 }
%}

%extend ctypename {
	%javamethodmodifiers java_init "protected";
	int java_init(jobject jsess) {
		implclass *session = (implclass *)$self;
		JAVA_CALLBACK *jcb = (JAVA_CALLBACK *)session->lang_private;
		jcb->jobj = JCALL1(NewGlobalRef, jcb->jnienv, jsess);
		JCALL1(DeleteLocalRef, jcb->jnienv, jsess);
		return (0);
	}
}
%enddef

TRACKED_CLASS(Session, __wt_session, wiredtigerJNI.Session_java_init, WT_SESSION_IMPL)
TRACKED_CLASS(Connection, __wt_connection, wiredtigerJNI.Connection_java_init, WT_CONNECTION_IMPL)
/* Note: Cursor incorporates the elements of TRACKED_CLASS into its
 * custom constructor and %extend clause.
 */

%include "wiredtiger.h"

/* Return new connections, sessions and cursors. */
%inline {
WT_CONNECTION *wiredtiger_open_wrap(JNIEnv *jenv, const char *home, const char *config) {
	extern WT_EVENT_HANDLER javaApiEventHandler;
	WT_CONNECTION *conn = NULL;
	WT_CONNECTION_IMPL *connimpl;
	JAVA_CALLBACK *jcb;
	int ret;
	if ((ret = wiredtiger_open(home, &javaApiEventHandler, config, &conn)) != 0)
		goto err;

	connimpl = (WT_CONNECTION_IMPL *)conn;
	if ((ret = __wt_calloc_def(connimpl->default_session, 1, &jcb)) != 0)
		goto err;

	jcb->jnienv = jenv;
	connimpl->lang_private = jcb;

err:	if (ret != 0)
		throwWiredTigerException(jenv, wiredtiger_strerror(ret));
	return conn;
}
}

%extend __wt_connection {
	WT_SESSION *open_session_wrap(JNIEnv *jenv, const char *config) {
		extern WT_EVENT_HANDLER javaApiEventHandler;
		WT_SESSION *session = NULL;
		WT_SESSION_IMPL *sessionimpl;
		JAVA_CALLBACK *jcb;
		int ret;

		if ((ret = $self->open_session($self, &javaApiEventHandler, config, &session)) != 0)
			goto err;

		sessionimpl = (WT_SESSION_IMPL *)session;
		if ((ret = __wt_calloc_def(sessionimpl, 1, &jcb)) != 0)
			goto err;

		jcb->jnienv = jenv;
		sessionimpl->lang_private = jcb;

err:		if (ret != 0)
			throwWiredTigerException(jenv, wiredtiger_strerror(ret));
		return session;
	}
}

%extend __wt_session {
	WT_CURSOR *open_cursor_wrap(JNIEnv *jenv, const char *uri, WT_CURSOR_NULLABLE *to_dup, const char *config) {
		WT_CURSOR *cursor = NULL;
		JAVA_CALLBACK *jcb;
		int ret;

		if ((ret = $self->open_cursor($self, uri, to_dup, config, &cursor)) != 0)
			goto err;

		cursor->flags |= WT_CURSTD_RAW;

		if ((ret = __wt_calloc_def((WT_SESSION_IMPL *)cursor->session,
			    1, &jcb)) != 0)
			goto err;

		jcb->jnienv = jenv;
		cursor->lang_private = jcb;

err:		if (ret != 0)
			throwWiredTigerException(jenv, wiredtiger_strerror(ret));
		return cursor;
	}
}
