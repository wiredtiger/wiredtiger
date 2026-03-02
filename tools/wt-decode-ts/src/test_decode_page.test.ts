import * as assert from 'assert';
import * as fs from 'fs';
import * as path from 'path';
import { BinaryFile } from './binary_data.js';
import { WTPage, PageType } from './btree_format.js';
import * as mdb_log_parse from './mdb_log_parse.js';

function testWtpageHeadersFromWiredtiger01() {
    const opts = {
        disagg: false,
        skipData: true,
        cont: false,
        debug: false,
        raw: false,
        output: null
    };

    const filePath = path.join(process.cwd(), '../test/binary_files/WiredTiger01.txt');
    const content = fs.readFileSync(filePath, 'utf8');
    const blocks = mdb_log_parse.extractBlocks(content, opts);
    const pageBytes = blocks[0]!;

    assert.ok(pageBytes.length > 0, "Encoded page bytes should not be empty");

    const b = new BinaryFile(pageBytes);
    const page = WTPage.parse(b, pageBytes.length, opts);

    assert.strictEqual(page.success, true, "WTPage parsing failed");

    const p = page.pageHeader!;
    assert.ok(p, "Page header missing");
    assert.strictEqual(p.recno, 0n);
    assert.strictEqual(p.writeGen, 11n);
    assert.strictEqual(p.memSize, 3702);
    assert.strictEqual(p.entries, 16);
    assert.strictEqual(p.type, PageType.WT_PAGE_ROW_LEAF);
    assert.strictEqual(p.flags, 4);
    assert.strictEqual(p.version, 1);

    const bh = page.blockHeader as any;
    assert.ok(bh, "Block header missing");
    assert.strictEqual(bh.diskSize, 4096);
    assert.strictEqual(bh.checksum, 414598985);
    assert.strictEqual(bh.flags, 1);

    console.log("testWtpageHeadersFromWiredtiger01 passed!");
}

try {
    testWtpageHeadersFromWiredtiger01();
} catch (e) {
    console.error("Test failed!");
    console.error(e);
    process.exit(1);
}
