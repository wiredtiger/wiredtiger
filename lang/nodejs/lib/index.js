const addon = require('../build/Release/wiredtiger.node');

class Connection {
    constructor(nativeConn) {
        this._native = nativeConn;
    }

    open_session(config = '') {
        return new Session(this._native.open_session(config));
    }

    close(config = '') {
        return this._native.close(config);
    }
}

class Session {
    constructor(nativeSession) {
        this._native = nativeSession;
    }

    open_cursor(uri, to_dup = null, config = '') {
        return new Cursor(this._native.open_cursor(uri, to_dup, config));
    }

    create(name, config = '') {
        return this._native.create(name, config);
    }

    drop(name, config = '') {
        return this._native.drop(name, config);
    }

    begin_transaction(config = '') {
        return this._native.begin_transaction(config);
    }

    commit_transaction(config = '') {
        return this._native.commit_transaction(config);
    }

    rollback_transaction(config = '') {
        return this._native.rollback_transaction(config);
    }

    close(config = '') {
        return this._native.close(config);
    }
}

class Cursor {
    constructor(nativeCursor) {
        this._native = nativeCursor;
        this._key = null;
        this._value = null;
    }

    next() { return this._native.next(); }
    prev() { return this._native.prev(); }
    reset() { return this._native.reset(); }
    search() { return this._native.search(); }
    
    get_key() { return this._native.get_key(); }
    get_value() { return this._native.get_value(); }
    
    set_key(key) {
        this._key = key; // Pin it
        this._native.set_key(key);
    }
    
    set_value(value) {
        this._value = value; // Pin it
        this._native.set_value(value);
    }
    
    insert() { return this._native.insert(); }
    update() { return this._native.update(); }
    remove() { return this._native.remove(); }
    close() { return this._native.close(); }

    // JS convenience
    [Symbol.iterator]() {
        return {
            next: () => {
                if (this.next() === addon.WT_NOTFOUND) {
                    return { done: true };
                }
                return {
                    value: [this.get_key(), this.get_value()],
                    done: false
                };
            }
        };
    }
}

module.exports = {
    open: (home, err_handler = null, config = '') => {
        const nativeConn = addon.open(home, err_handler, config);
        return new Connection(nativeConn);
    },
    version: addon.version,
    strerror: addon.strerror,
    WT_NOTFOUND: addon.WT_NOTFOUND,
    WT_ROLLBACK: addon.WT_ROLLBACK,
    WT_DUPLICATE_KEY: addon.WT_DUPLICATE_KEY
};
