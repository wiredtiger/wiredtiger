import * as assert from 'assert';
import * as fs from 'fs';
import * as path from 'path';
import { BinaryFile } from './binary_data.js';
import { WTPage, PageType, BlockDisaggHeader } from './btree_format.js';
import * as mdb_log_parse from './mdb_log_parse.js';
import * as page_service from './page_service.js';

const BINARY_FILES_DIR = path.join(process.cwd(), '../test/binary_files');

function testWiredTiger01() {
    console.log("Testing WiredTiger01.txt...");
    const opts = { disagg: false, skipData: true };
    const filePath = path.join(BINARY_FILES_DIR, 'WiredTiger01.txt');
    const content = fs.readFileSync(filePath, 'utf8');
    const blocks = mdb_log_parse.extractBlocks(content, opts);
    const pageBytes = blocks[0]!;

    assert.ok(pageBytes.length > 0, "Encoded page bytes should not be empty");
    const b = new BinaryFile(pageBytes);
    const page = WTPage.parse(b, pageBytes.length, opts);

    assert.strictEqual(page.success, true);
    assert.strictEqual(page.pageHeader?.type, PageType.WT_PAGE_ROW_LEAF);
    assert.strictEqual(page.blockHeader?.checksum, 0x18b64749);
    console.log("  Passed!");
}

function testDisaggDeltaChain() {
    console.log("Testing disagg_delta_chain.log...");
    const opts = { disagg: true, skipData: true, quietDisaggDetection: true };
    const filePath = path.join(BINARY_FILES_DIR, 'disagg_delta_chain.log');
    const content = fs.readFileSync(filePath, 'utf8');
    
    const blocks = mdb_log_parse.extractBlocks(content, opts);
    assert.ok(blocks.length > 0);
    
    for (const pageBytes of blocks) {
        const b = new BinaryFile(pageBytes);
        const page = WTPage.parse(b, b.length, opts);
        assert.ok(page.success);
    }
    console.log(`  Processed ${blocks.length} pages from delta chain.`);
}

function testDisaggOplogJsonl() {
    console.log("Testing disagg_oplog.jsonl...");
    const opts = { json: true };
    const filePath = path.join(BINARY_FILES_DIR, 'disagg_oplog.jsonl');
    const content = fs.readFileSync(filePath, 'utf8');
    const lines = content.split('\n');
    
    // Capture stdout to avoid cluttering test output
    const oldLog = console.log;
    let output = '';
    console.log = (s: string) => { output += s; };
    
    try {
        page_service.processDisaggTable(lines, opts);
        const data = JSON.parse(output);
        assert.ok(Array.isArray(data));
        // Verify we have some entries
        assert.ok(data.length > 0);
    } finally {
        console.log = oldLog;
    }
    console.log("  Passed!");
}

function testCorruptionCases() {
    console.log("Testing corruption cases...");
    const opts = { debug: false };
    
    const cases = [
        { file: 'mongodb_non_hex.log', expectedError: 'Hex dump is corrupt' },
        { file: 'mongodb_odd_length.log', expectedError: 'Hex dump is corrupt' },
        { file: 'mongodb_size_mismatch.log', expectedError: 'Hex dump is corrupt' }
    ];

    for (const c of cases) {
        const filePath = path.join(BINARY_FILES_DIR, c.file);
        const content = fs.readFileSync(filePath, 'utf8');
        
        const oldLog = console.log;
        let output = '';
        console.log = (s: string) => { output += s; };
        
        try {
            const blocks = mdb_log_parse.extractBlocks(content, opts);
            assert.strictEqual(blocks.length, 0, `File ${c.file} should return 0 blocks due to corruption`);
            assert.ok(output.includes(c.expectedError), `File ${c.file} should log "${c.expectedError}"`);
        } finally {
            console.log = oldLog;
        }
        console.log(`  ${c.file} Passed!`);
    }
}

function testDisaggDeltaOplogBin() {
    console.log("Testing disagg_delta_oplog.bin...");
    const opts = { disagg: true, skipData: false };
    const filePath = path.join(BINARY_FILES_DIR, 'disagg_delta_oplog.bin');
    const buffer = fs.readFileSync(filePath);
    const b = new BinaryFile(buffer);
    
    const page = WTPage.parse(b, buffer.length, opts);
    assert.strictEqual(page.success, true);
    assert.strictEqual(page.pageHeader?.type, PageType.WT_PAGE_ROW_LEAF);
    assert.strictEqual(page.pageHeader?.entries, 2);
    assert.ok(page.blockHeader instanceof BlockDisaggHeader);
    assert.strictEqual(page.blockHeader.magic, BlockDisaggHeader.WT_BLOCK_DISAGG_MAGIC_DELTA);
    console.log("  Passed!");
}

async function runAll() {
    try {
        testWiredTiger01();
        testDisaggDeltaChain();
        testDisaggOplogJsonl();
        testCorruptionCases();
        testDisaggDeltaOplogBin();
        console.log("\nAll tests passed successfully!");
    } catch (e) {
        console.error("\nTests failed!");
        console.error(e);
        process.exit(1);
    }
}

runAll();
