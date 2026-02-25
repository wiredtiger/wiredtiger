const wt = require('../lib/index');
const fs = require('fs');

const home = 'WT_HOME_TXN';
if (fs.existsSync(home)) {
    fs.rmSync(home, { recursive: true, force: true });
}
fs.mkdirSync(home);

try {
    const conn = wt.open(home, null, 'create');
    const session = conn.open_session();
    session.create('table:test', 'key_format=u,value_format=u');

    // Transaction 1: Commit
    session.begin_transaction();
    const cursor = session.open_cursor('table:test');
    cursor.set_key('txn1');
    cursor.set_value('value1');
    cursor.insert();
    session.commit_transaction();
    console.log('Committed txn1');

    // Transaction 2: Rollback
    session.begin_transaction();
    cursor.set_key('txn2');
    cursor.set_value('value2');
    cursor.insert();
    session.rollback_transaction();
    console.log('Rolled back txn2');

    // Verify
    console.log('Final table content:');
    cursor.reset();
    for (const [key, value] of cursor) {
        console.log(`  ${key}: ${value}`);
    }

    cursor.close();
    session.close();
    conn.close();
} catch (err) {
    console.error('Error:', err);
}
