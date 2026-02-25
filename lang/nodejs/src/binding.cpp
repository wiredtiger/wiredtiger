#include <napi.h>
#include <wiredtiger.h>
#include <string>
#include <vector>

class Cursor : public Napi::ObjectWrap<Cursor> {
public:
    static Napi::Object Init(Napi::Env env, Napi::Object exports);
    Cursor(const Napi::CallbackInfo& info);
    void SetCursor(WT_CURSOR* cursor) { _cursor = cursor; }

    static Napi::FunctionReference constructor;

private:

    Napi::Value Next(const Napi::CallbackInfo& info);
    Napi::Value Prev(const Napi::CallbackInfo& info);
    Napi::Value Reset(const Napi::CallbackInfo& info);
    Napi::Value Search(const Napi::CallbackInfo& info);
    void SetKey(const Napi::CallbackInfo& info);
    void SetValue(const Napi::CallbackInfo& info);
    Napi::Value GetKey(const Napi::CallbackInfo& info);
    Napi::Value GetValue(const Napi::CallbackInfo& info);
    Napi::Value Insert(const Napi::CallbackInfo& info);
    Napi::Value Update(const Napi::CallbackInfo& info);
    Napi::Value Remove(const Napi::CallbackInfo& info);
    Napi::Value Close(const Napi::CallbackInfo& info);

    WT_CURSOR* _cursor;
    std::string _key_str;
    std::string _value_str;
};

class Session : public Napi::ObjectWrap<Session> {
public:
    static Napi::Object Init(Napi::Env env, Napi::Object exports);
    Session(const Napi::CallbackInfo& info);
    void SetSession(WT_SESSION* session) { _session = session; }

    static Napi::FunctionReference constructor;

private:

    Napi::Value OpenCursor(const Napi::CallbackInfo& info);
    Napi::Value Create(const Napi::CallbackInfo& info);
    Napi::Value Drop(const Napi::CallbackInfo& info);
    Napi::Value BeginTransaction(const Napi::CallbackInfo& info);
    Napi::Value CommitTransaction(const Napi::CallbackInfo& info);
    Napi::Value RollbackTransaction(const Napi::CallbackInfo& info);
    Napi::Value Close(const Napi::CallbackInfo& info);

    WT_SESSION* _session;
};

class Connection : public Napi::ObjectWrap<Connection> {
public:
    static Napi::Object Init(Napi::Env env, Napi::Object exports);
    Connection(const Napi::CallbackInfo& info);
    void SetConnection(WT_CONNECTION* conn) { _conn = conn; }

    static Napi::FunctionReference constructor;

private:

    Napi::Value OpenSession(const Napi::CallbackInfo& info);
    Napi::Value Close(const Napi::CallbackInfo& info);

    WT_CONNECTION* _conn;
};

// --- Connection Implementation ---

Napi::FunctionReference Connection::constructor;

Napi::Object Connection::Init(Napi::Env env, Napi::Object exports) {
    Napi::HandleScope scope(env);
    Napi::Function func = DefineClass(env, "Connection", {
        InstanceMethod("open_session", &Connection::OpenSession),
        InstanceMethod("close", &Connection::Close),
    });
    constructor = Napi::Persistent(func);
    constructor.SuppressDestruct();
    exports.Set("Connection", func);
    return exports;
}

Connection::Connection(const Napi::CallbackInfo& info) : Napi::ObjectWrap<Connection>(info), _conn(nullptr) {}

Napi::Value Connection::OpenSession(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    std::string config = info.Length() > 0 ? info[0].As<Napi::String>().Utf8Value() : "";
    
    WT_SESSION* session;
    int ret = _conn->open_session(_conn, nullptr, config.c_str(), &session);
    if (ret != 0) {
        throw Napi::Error::New(env, wiredtiger_strerror(ret));
    }

    Napi::Object sessionObj = Session::constructor.New({});
    Session* s = Napi::ObjectWrap<Session>::Unwrap(sessionObj);
    s->SetSession(session);
    return sessionObj;
}

Napi::Value Connection::Close(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    std::string config = info.Length() > 0 ? info[0].As<Napi::String>().Utf8Value() : "";
    int ret = _conn->close(_conn, config.c_str());
    _conn = nullptr;
    return Napi::Number::New(env, ret);
}

// --- Session Implementation ---

Napi::FunctionReference Session::constructor;

Napi::Object Session::Init(Napi::Env env, Napi::Object exports) {
    Napi::HandleScope scope(env);
    Napi::Function func = DefineClass(env, "Session", {
        InstanceMethod("open_cursor", &Session::OpenCursor),
        InstanceMethod("create", &Session::Create),
        InstanceMethod("drop", &Session::Drop),
        InstanceMethod("begin_transaction", &Session::BeginTransaction),
        InstanceMethod("commit_transaction", &Session::CommitTransaction),
        InstanceMethod("rollback_transaction", &Session::RollbackTransaction),
        InstanceMethod("close", &Session::Close),
    });
    constructor = Napi::Persistent(func);
    constructor.SuppressDestruct();
    exports.Set("Session", func);
    return exports;
}

Session::Session(const Napi::CallbackInfo& info) : Napi::ObjectWrap<Session>(info), _session(nullptr) {}

Napi::Value Session::OpenCursor(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    std::string uri = info[0].As<Napi::String>().Utf8Value();
    std::string config = info.Length() > 2 ? info[2].As<Napi::String>().Utf8Value() : "";
    
    WT_CURSOR* cursor;
    int ret = _session->open_cursor(_session, uri.c_str(), nullptr, config.c_str(), &cursor);
    if (ret != 0) {
        throw Napi::Error::New(env, wiredtiger_strerror(ret));
    }

    // Set RAW flag to use WT_ITEM for get/set
    cursor->flags |= WT_CURSTD_RAW;

    Napi::Object cursorObj = Cursor::constructor.New({});
    Cursor* c = Napi::ObjectWrap<Cursor>::Unwrap(cursorObj);
    c->SetCursor(cursor);
    return cursorObj;
}

Napi::Value Session::Create(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    std::string name = info[0].As<Napi::String>().Utf8Value();
    std::string config = info.Length() > 1 ? info[1].As<Napi::String>().Utf8Value() : "";
    int ret = _session->create(_session, name.c_str(), config.c_str());
    if (ret != 0) throw Napi::Error::New(env, wiredtiger_strerror(ret));
    return Napi::Number::New(env, ret);
}

Napi::Value Session::Drop(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    std::string name = info[0].As<Napi::String>().Utf8Value();
    std::string config = info.Length() > 1 ? info[1].As<Napi::String>().Utf8Value() : "";
    int ret = _session->drop(_session, name.c_str(), config.c_str());
    if (ret != 0) throw Napi::Error::New(env, wiredtiger_strerror(ret));
    return Napi::Number::New(env, ret);
}

Napi::Value Session::BeginTransaction(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    std::string config = info.Length() > 0 ? info[0].As<Napi::String>().Utf8Value() : "";
    int ret = _session->begin_transaction(_session, config.c_str());
    if (ret != 0) throw Napi::Error::New(env, wiredtiger_strerror(ret));
    return Napi::Number::New(env, ret);
}

Napi::Value Session::CommitTransaction(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    std::string config = info.Length() > 0 ? info[0].As<Napi::String>().Utf8Value() : "";
    int ret = _session->commit_transaction(_session, config.c_str());
    if (ret != 0) throw Napi::Error::New(env, wiredtiger_strerror(ret));
    return Napi::Number::New(env, ret);
}

Napi::Value Session::RollbackTransaction(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    std::string config = info.Length() > 0 ? info[0].As<Napi::String>().Utf8Value() : "";
    int ret = _session->rollback_transaction(_session, config.c_str());
    if (ret != 0) throw Napi::Error::New(env, wiredtiger_strerror(ret));
    return Napi::Number::New(env, ret);
}

Napi::Value Session::Close(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    std::string config = info.Length() > 0 ? info[0].As<Napi::String>().Utf8Value() : "";
    int ret = _session->close(_session, config.c_str());
    _session = nullptr;
    return Napi::Number::New(env, ret);
}

// --- Cursor Implementation ---

Napi::FunctionReference Cursor::constructor;

Napi::Object Cursor::Init(Napi::Env env, Napi::Object exports) {
    Napi::HandleScope scope(env);
    Napi::Function func = DefineClass(env, "Cursor", {
        InstanceMethod("next", &Cursor::Next),
        InstanceMethod("prev", &Cursor::Prev),
        InstanceMethod("reset", &Cursor::Reset),
        InstanceMethod("search", &Cursor::Search),
        InstanceMethod("set_key", &Cursor::SetKey),
        InstanceMethod("set_value", &Cursor::SetValue),
        InstanceMethod("get_key", &Cursor::GetKey),
        InstanceMethod("get_value", &Cursor::GetValue),
        InstanceMethod("insert", &Cursor::Insert),
        InstanceMethod("update", &Cursor::Update),
        InstanceMethod("remove", &Cursor::Remove),
        InstanceMethod("close", &Cursor::Close),
    });
    constructor = Napi::Persistent(func);
    constructor.SuppressDestruct();
    exports.Set("Cursor", func);
    return exports;
}

Cursor::Cursor(const Napi::CallbackInfo& info) : Napi::ObjectWrap<Cursor>(info), _cursor(nullptr) {}

Napi::Value Cursor::Next(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    int ret = _cursor->next(_cursor);
    return Napi::Number::New(env, ret);
}

Napi::Value Cursor::Prev(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    int ret = _cursor->prev(_cursor);
    return Napi::Number::New(env, ret);
}

Napi::Value Cursor::Reset(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    int ret = _cursor->reset(_cursor);
    return Napi::Number::New(env, ret);
}

Napi::Value Cursor::Search(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    int ret = _cursor->search(_cursor);
    return Napi::Number::New(env, ret);
}

void Cursor::SetKey(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    WT_ITEM item;
    if (info[0].IsString()) {
        _key_str = info[0].As<Napi::String>().Utf8Value();
        item.data = _key_str.c_str();
        item.size = _key_str.length();
        _cursor->set_key(_cursor, &item);
    } else if (info[0].IsBuffer()) {
        Napi::Buffer<char> buf = info[0].As<Napi::Buffer<char>>();
        item.data = buf.Data();
        item.size = buf.Length();
        _cursor->set_key(_cursor, &item);
    }
}

void Cursor::SetValue(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    WT_ITEM item;
    if (info[0].IsString()) {
        _value_str = info[0].As<Napi::String>().Utf8Value();
        item.data = _value_str.c_str();
        item.size = _value_str.length();
        _cursor->set_value(_cursor, &item);
    } else if (info[0].IsBuffer()) {
        Napi::Buffer<char> buf = info[0].As<Napi::Buffer<char>>();
        item.data = buf.Data();
        item.size = buf.Length();
        _cursor->set_value(_cursor, &item);
    }
}

Napi::Value Cursor::GetKey(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    WT_ITEM item;
    int ret = _cursor->get_key(_cursor, &item);
    if (ret != 0) throw Napi::Error::New(env, wiredtiger_strerror(ret));
    return Napi::String::New(env, (const char*)item.data, item.size);
}

Napi::Value Cursor::GetValue(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    WT_ITEM item;
    int ret = _cursor->get_value(_cursor, &item);
    if (ret != 0) throw Napi::Error::New(env, wiredtiger_strerror(ret));
    return Napi::String::New(env, (const char*)item.data, item.size);
}

Napi::Value Cursor::Insert(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    int ret = _cursor->insert(_cursor);
    return Napi::Number::New(env, ret);
}

Napi::Value Cursor::Update(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    int ret = _cursor->update(_cursor);
    return Napi::Number::New(env, ret);
}

Napi::Value Cursor::Remove(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    int ret = _cursor->remove(_cursor);
    return Napi::Number::New(env, ret);
}

Napi::Value Cursor::Close(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    int ret = _cursor->close(_cursor);
    _cursor = nullptr;
    return Napi::Number::New(env, ret);
}

// --- Module Init ---

Napi::Value Open(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    std::string home = info[0].As<Napi::String>().Utf8Value();
    std::string config = info.Length() > 2 ? info[2].As<Napi::String>().Utf8Value() : "";

    WT_CONNECTION* conn;
    int ret = wiredtiger_open(home.c_str(), nullptr, config.c_str(), &conn);
    if (ret != 0) {
        throw Napi::Error::New(env, wiredtiger_strerror(ret));
    }

    Napi::Object connObj = Connection::constructor.New({});
    Connection* c = Napi::ObjectWrap<Connection>::Unwrap(connObj);
    c->SetConnection(conn);
    return connObj;
}

Napi::Value Version(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    int major, minor, patch;
    const char* version = wiredtiger_version(&major, &minor, &patch);
    return Napi::String::New(env, version);
}

Napi::Value StrError(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();
    int err = info[0].As<Napi::Number>().Int32Value();
    return Napi::String::New(env, wiredtiger_strerror(err));
}

Napi::Object Init(Napi::Env env, Napi::Object exports) {
    Connection::Init(env, exports);
    Session::Init(env, exports);
    Cursor::Init(env, exports);

    exports.Set("open", Napi::Function::New(env, Open));
    exports.Set("version", Napi::Function::New(env, Version));
    exports.Set("strerror", Napi::Function::New(env, StrError));
    exports.Set("WT_NOTFOUND", Napi::Number::New(env, WT_NOTFOUND));
    exports.Set("WT_ROLLBACK", Napi::Number::New(env, WT_ROLLBACK));
    exports.Set("WT_DUPLICATE_KEY", Napi::Number::New(env, WT_DUPLICATE_KEY));
    
    return exports;
}

NODE_API_MODULE(wiredtiger, Init)
