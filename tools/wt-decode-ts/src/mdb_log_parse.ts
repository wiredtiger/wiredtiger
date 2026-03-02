import { BinaryFile } from './binary_data.js';

export function validateHexData(hexdata: string): void {
    if (/[^0-9a-fA-F\s]/.test(hexdata)) {
        throw new Error('Non-hex characters found');
    }
    const clean = hexdata.replace(/[^0-9a-fA-F]/g, '');
    if (clean.length % 2 !== 0) {
        throw new Error('Hex data chunk length is not even');
    }
}

export function extractAllMongodbBlocks(lines: string[], opts: any): Buffer[] {
    const allBlocks: Buffer[] = [];
    let currentChunks: Buffer[] = [];
    let expectedSize = 0;
    let expectedChunks = 0;

    for (const line of lines) {
        if (!line.trim()) continue;
        try {
            const entry = JSON.parse(line);
            const msg = entry?.attr?.message?.msg || '';
            if (msg.includes('__wt_bm_corrupt_dump')) {
                const match = msg.match(/\{0:\s*(\d+),\s*(\d+),\s*(0x[0-9a-f]+)\}:\s*\(chunk\s+(\d+)\s+of\s+(\d+)\):\s*(.+$)/);
                if (match) {
                    const [_, offset, size, checksum, chunkNum, totalChunks, hexdata] = match;
                    const nSize = parseInt(size);
                    const nChunkNum = parseInt(chunkNum);
                    const nTotalChunks = parseInt(totalChunks);

                    if (nChunkNum === 1) {
                        currentChunks = [];
                        expectedSize = nSize;
                        expectedChunks = nTotalChunks;
                    }

                    validateHexData(hexdata);
                    const cleanHex = hexdata.replace(/[^0-9a-fA-F]/g, '');
                    currentChunks.push(Buffer.from(cleanHex, 'hex'));

                    if (currentChunks.length === expectedChunks) {
                        const totalLen = currentChunks.reduce((acc, chunk) => acc + chunk.length, 0);
                        if (totalLen !== expectedSize) {
                            console.log(`Hex dump is corrupt - Block size mismatch: expected ${expectedSize}, got ${totalLen}`);
                        } else {
                            allBlocks.push(Buffer.concat(currentChunks));
                        }
                        currentChunks = [];
                    }
                }
            }
        } catch (e: any) {
            if (!(e instanceof SyntaxError)) {
                console.log(`Hex dump is corrupt - ${e.message}`);
            }
        }
    }
    return allBlocks;
}

export function extractAllWiredTigerBlocks(lines: string[], opts: any): Buffer[] {
    const allBlocks: Buffer[] = [];
    let currentBlock = Buffer.alloc(0);
    let expectedChunks: number | null = null;
    let currentChunkCount = 0;

    for (let line of lines) {
        if (!line.trim()) continue;
        const rawLine = line;
        
        const chunkMatch = rawLine.match(/\(chunk\s+(\d+)\s+of\s+(\d+)\)/);
        if (chunkMatch) {
            const chunkNum = parseInt(chunkMatch[1]!);
            const totalChunks = parseInt(chunkMatch[2]!);
            
            if (chunkNum === 1) {
                if (currentBlock.length > 0) allBlocks.push(currentBlock);
                currentBlock = Buffer.alloc(0);
                expectedChunks = totalChunks;
                currentChunkCount = 0;
            }
            
            if (line.includes(':')) {
                line = line.split(':').pop() || '';
            }
            const nospace = line.replace(/[^a-fA-F0-9]/g, '');
            currentBlock = Buffer.concat([currentBlock, Buffer.from(nospace, 'hex')]);
            currentChunkCount++;
            
            if (expectedChunks !== null && currentChunkCount >= expectedChunks) {
                allBlocks.push(currentBlock);
                currentBlock = Buffer.alloc(0);
                expectedChunks = null;
            }
        } else {
            // Check if this line looks like a single-line hex dump
            const nospace = line.replace(/[^a-fA-F0-9]/g, '');
            if (nospace.length >= 32 && /^[a-fA-F0-9]+$/.test(nospace)) {
                allBlocks.push(Buffer.from(nospace, 'hex'));
            }
        }
    }
    if (currentBlock.length > 0) allBlocks.push(currentBlock);
    return allBlocks;
}

export function extractBlocks(content: string, opts: any): Buffer[] {
    const lines = content.split('\n');
    const firstLine = lines.find(l => l.trim().length > 0);
    
    if (firstLine && firstLine.trim().startsWith('{')) {
        const blocks = extractAllMongodbBlocks(lines, opts);
        if (blocks.length === 0) {
            console.log('Error: No valid byte dump found in MongoDB log');
        }
        return blocks;
    }

    const blocks = extractAllWiredTigerBlocks(lines, opts);
    if (blocks.length === 0) {
        console.log('Error: No byte dumps found in WiredTiger log');
    }
    return blocks;
}
