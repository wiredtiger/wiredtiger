#include <ostream>
#include <string>
#include <vector>

namespace workgen {

struct Table {
	int _tableid;

	/* XXX select table from range */

	Table() {}
	Table(int tableid) : _tableid(tableid) {}
	Table(const Table &other) : _tableid(other._tableid) {}
	~Table() {}

    void describe(std::ostream &os) const { os << "Table: " << _tableid; }
};

struct Key {
	typedef enum { KEYGEN_APPEND, KEYGEN_PARETO, KEYGEN_UNIFORM } KeyType;
	KeyType _keytype;
	int _size;

	/* XXX specify more about key distribution */
	Key() {}
	Key(KeyType keytype, int size) : _keytype(keytype), _size(size) {}
	Key(const Key &other) : _keytype(other._keytype), _size(other._size) {}
	~Key() {}

    void describe(std::ostream &os) const { os << "Key: " << _keytype << ", size " << _size; }
};

struct Value {
	int _size;

	/* XXX specify how value is calculated */
	Value() {}
	Value(int size) : _size(size) {}
	Value(const Value &other) : _size(other._size) {}
	~Value() {}

    void describe(std::ostream &os) const { os << "Value: " << _size; }
};

struct Operation {
	typedef enum { OP_INSERT, OP_REMOVE, OP_SEARCH, OP_UPDATE } OpType;
	OpType _optype;

	Table _table;
	Key _key;
	Value _value;

	Operation() {}
	Operation(OpType optype, Table table, Key key, Value value) : _optype(optype), _table(table), _key(key), _value(value) {}
	Operation(const Operation &other) : _optype(other._optype), _table(other._table), _key(other._key), _value(other._value) {}
	~Operation() {}

    void describe(std::ostream &os) const {
        os << "Operation: " << _optype << ", "; _table.describe(os);
        os << ", "; _key.describe(os); os << ", "; _value.describe(os);
    }
};

struct Thread {
	std::vector<Operation> _ops;
	
	/* XXX throttle info, etc. */

	Thread() {}
	Thread(std::vector<Operation> &ops) : _ops(ops) {}
	Thread(const Thread &other) : _ops(other._ops) {}
	~Thread() {}

    void describe(std::ostream &os) const {
        os << "Thread: [" << std::endl;
        for (std::vector<Operation>::const_iterator i = _ops.begin(); i != _ops.end(); i++) {
            os << "  "; i->describe(os); os << std::endl;
        }
        os << "]";
    }
};

struct Workload {
	std::vector<Thread> _threads;

	Workload(std::vector<Thread> &threads) : _threads(threads) {}
	Workload(const Workload &other) : _threads(other._threads) {}
	~Workload() {}

    void describe(std::ostream &os) const {
        os << "Workload: [" << std::endl;
        for (std::vector<Thread>::const_iterator i = _threads.begin(); i != _threads.end(); i++) {
            os << "  "; i->describe(os); os << std::endl;
        }
        os << "]";
    }
};

int execute(Workload &workload);

};
