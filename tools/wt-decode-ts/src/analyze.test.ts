import * as assert from 'assert';
import * as fs from 'fs';
import * as path from 'path';
import { extractRootOffsetFromAddr, collectMetadata } from './analyze.js';

async function testAnalyzeBug033() {
    console.log("Testing analyze functionality with test_bug033.0...");
    
    const homeDir = path.join(process.cwd(), '../test/binary_files/test_bug033.0');
    assert.ok(fs.existsSync(homeDir), `Home directory ${homeDir} missing`);

    const turtlePath = path.join(homeDir, 'WiredTiger.turtle');
    const turtleContent = fs.readFileSync(turtlePath, 'utf8');
    
    let metaAddr: string | null = null;
    const lines = turtleContent.split('\n');
    for (let i = 0; i < lines.length; i++) {
        const line = lines[i]!;
        if (line.includes('file:WiredTiger.wt')) {
            const match = line.match(/addr="([^"]+)"/);
            if (match) {
                metaAddr = match[1]!;
                break;
            }
            const nextLine = lines[i+1] || '';
            const match2 = nextLine.match(/addr="([^"]+)"/);
            if (match2) {
                metaAddr = match2[1]!;
                break;
            }
        }
    }

    assert.ok(metaAddr, "Could not find metadata address in turtle file");
    console.log(`  Metadata address: ${metaAddr}`);

    const metaRootOffset = extractRootOffsetFromAddr(metaAddr!);
    assert.ok(metaRootOffset !== null, "Could not extract root offset from metadata address");
    console.log(`  Metadata root offset: 0x${metaRootOffset!.toString(16)}`);

    const metaFilePath = path.join(homeDir, 'WiredTiger.wt');
    const metadata = await collectMetadata(metaFilePath, metaRootOffset!);
    
    const files = Object.keys(metadata).filter(k => k.startsWith('file:') || k.startsWith('table:'));
    assert.ok(files.includes('file:test_bug033.wt'), "test_bug033.wt not found in metadata");
    console.log(`  Found ${files.length} entries in metadata.`);

    const bug033Config = metadata['file:test_bug033.wt']!;
    assert.ok(bug033Config.includes('access_pattern_hint=none'), "Incorrect config for test_bug033.wt");
    
    console.log("  analyze test passed!");
}

testAnalyzeBug033().catch(e => {
    console.error("Test failed!");
    console.error(e);
    process.exit(1);
});
