const wt = require('../lib/index');
const fs = require('fs');

const home = 'WT_HOME';
if (fs.existsSync(home)) {
    fs.rmSync(home, { recursive: true, force: true });
}
fs.mkdirSync(home);

try {
    console.log('WiredTiger Version:', wt.version());

    const conn = wt.open(home, null, 'create');
    console.log('Connected to WiredTiger');

    const session = conn.open_session();
    console.log('Opened session');

    session.create('table:test', 'key_format=u,value_format=u');
    console.log('Created table');

    const cursor = session.open_cursor('table:test');
    console.log('Opened cursor');

    cursor.set_key('key1');
    cursor.set_value('value1');
    cursor.insert();
    console.log('Inserted key1');

    cursor.set_key('key2');
    cursor.set_value('value2');
    cursor.insert();
    console.log('Inserted key2');

    console.log('Iterating over table:');
    cursor.reset();
    for (const [key, value] of cursor) {
        console.log(`  ${key}: ${value}`);
    }

    cursor.close();
    session.close();
    conn.close();
    console.log('Closed everything');

} catch (err) {
    console.error('Error:', err);
}
