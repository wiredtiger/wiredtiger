import * as fs from 'fs';
import * as path from 'path';
import chalk from 'chalk';
import inquirer from 'inquirer';
import { BinaryFile, unpackInt } from './binary_data.js';
import { WTPage, PageType, Cell, BlockDisaggHeader } from './btree_format.js';
import { rawBytes } from './printer.js';

export async function analyzeHome(homeDir: string) {
    if (!fs.existsSync(homeDir)) {
        console.error(chalk.red(`Error: Directory ${homeDir} does not exist.`));
        return;
    }

    const turtlePath = path.join(homeDir, 'WiredTiger.turtle');
    if (!fs.existsSync(turtlePath)) {
        console.error(chalk.red(`Error: WiredTiger.turtle not found in ${homeDir}`));
        return;
    }

    console.log(chalk.bold.cyan(`Analyzing WiredTiger Home: ${homeDir}`));
    
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

    if (!metaAddr) {
        console.error(chalk.red("Could not find metadata file address in WiredTiger.turtle"));
        return;
    }

    const metaInfo = decodeFullAddr(metaAddr);
    if (!metaInfo) {
        console.error(chalk.red("Could not decode metadata root offset"));
        return;
    }

    const metaFilePath = path.join(homeDir, 'WiredTiger.wt');
    const metadata = await collectMetadata(metaFilePath, metaInfo.root.offset);
    
    const files = Object.keys(metadata).filter(k => k.startsWith('file:') || k.startsWith('table:'));
    
    if (files.length === 0) {
        console.log(chalk.yellow("No files found in metadata."));
        return;
    }

    while (true) {
        const { selectedFile } = await inquirer.prompt([
            {
                type: 'list',
                name: 'selectedFile',
                message: 'Select a file or table to inspect:',
                pageSize: 20,
                choices: [...files.sort(), new inquirer.Separator(), 'Exit']
            }
        ]);

        if (selectedFile === 'Exit') break;

        await inspectFile(homeDir, selectedFile, metadata[selectedFile]!);
    }
}

export function extractRootOffsetFromAddr(addrHex: string, allocSize: number = 4096): number | null {
    const info = decodeFullAddr(addrHex, allocSize);
    return info ? info.root.offset : null;
}

interface AddrRef {
    offset: number;
    size: number;
    cksum: number;
}

interface CheckpointInfo {
    root: AddrRef;
    alloc: AddrRef;
    avail: AddrRef;
    discard: AddrRef;
    fileSize: number;
    ckptSize: number;
}

function decodeFullAddr(addrHex: string, allocSize: number = 4096): CheckpointInfo | null {
    const buffer = Buffer.from(addrHex, 'hex');
    if (buffer[0] !== 1) return null;
    
    let rest = buffer.slice(1);
    const ints: bigint[] = [];
    while (rest.length > 0) {
        try {
            const [i, next] = unpackInt(rest as any);
            ints.push(i);
            rest = next as any;
        } catch (e) { break; }
    }

    if (ints.length < 14) return null;
    const refCnt = ints.length === 14 ? 3 : 4;
    
    const getRef = (pos: number): AddrRef => {
        const ref = ints.slice(pos, pos + refCnt);
        const data = ref.length === 4 ? ref.slice(1) : ref;
        const off = Number(data[0]!);
        const size = Number(data[1]!);
        const cksum = Number(data[2]!);
        return {
            offset: (off + 1) * allocSize,
            size: size * allocSize,
            cksum: cksum
        };
    };

    return {
        root: getRef(0),
        alloc: getRef(refCnt),
        avail: getRef(refCnt * 2),
        discard: getRef(refCnt * 3),
        fileSize: Number(ints[refCnt * 4]!),
        ckptSize: Number(ints[refCnt * 4 + 1]!)
    };
}

export async function collectMetadata(filePath: string, rootOffset: number): Promise<Record<string, string>> {
    const buffer = fs.readFileSync(filePath);
    const results: Record<string, string> = {};
    const queue: [number, number][] = [[rootOffset, 4096]]; 
    const seen = new Set<number>();

    while (queue.length > 0) {
        const [offset, size] = queue.shift()!;
        if (seen.has(offset)) continue;
        seen.add(offset);

        const b = new BinaryFile(buffer);
        b.seek(offset);
        
        try {
            const page = WTPage.parse(b, size, { disagg: false, skipData: false, quietDisaggDetection: true });
            if (!page.success) continue;

            if (page.pageHeader?.type === PageType.WT_PAGE_ROW_INT) {
                for (const cell of page.cells) {
                    if (cell.isAddress) {
                        const [off, sz] = decodeAddressCookie(cell.data);
                        queue.push([(off + 1) * 4096, sz * 4096]);
                    }
                }
            } else if (page.pageHeader?.type === PageType.WT_PAGE_ROW_LEAF) {
                for (let i = 0; i < page.cells.length; i += 2) {
                    const keyCell = page.cells[i];
                    const valCell = page.cells[i+1];
                    if (keyCell && valCell && keyCell.isKey && valCell.isValue) {
                        const key = keyCell.data.toString('utf8').replace(/\0/g, '');
                        const val = valCell.data.toString('utf8').replace(/\0/g, '');
                        results[key] = val;
                    }
                }
            }
        } catch (e) { }
    }

    return results;
}

function decodeAddressCookie(data: Buffer): [number, number, number] {
    try {
        const [off, rest] = unpackInt(data);
        const [size, rest2] = unpackInt(rest);
        const [cksum] = unpackInt(rest2);
        return [Number(off), Number(size), Number(cksum)];
    } catch (e) {
        return [0, 0, 0];
    }
}

async function inspectFile(homeDir: string, fileName: string, config: string) {
    console.log(chalk.bold.green(`\n--- Inspecting: ${fileName} ---`));
    
    const addrMatch = config.match(/checkpoint=\(WiredTigerCheckpoint\.\d+=\(addr="([^"]+)"/);
    if (!addrMatch) {
        console.log(chalk.red("Could not find checkpoint address for this entry."));
        return;
    }

    const addr = addrMatch[1]!;
    const info = decodeFullAddr(addr);
    if (!info) {
        console.log(chalk.red("Could not decode checkpoint information."));
        return;
    }

    let actualFileName = fileName;
    if (fileName.startsWith('file:')) {
        actualFileName = fileName.replace('file:', '');
    } else if (fileName.startsWith('table:')) {
        console.log(chalk.yellow("This is a table entry. Please select the corresponding file: entry to inspect data."));
        return;
    }

    const filePath = path.join(homeDir, actualFileName);
    if (!fs.existsSync(filePath)) {
        console.log(chalk.red(`File not found: ${filePath}`));
        return;
    }

    const buffer = fs.readFileSync(filePath);
    
    console.log(chalk.bold.yellow("\nBlock Manager Summary:"));
    console.log(`  File Size: ${info.fileSize} bytes`);
    console.log(`  Checkpoint Size: ${info.ckptSize} bytes`);
    
    const printListSummary = (name: string, ref: AddrRef) => {
        if (ref.size === 0) {
            console.log(chalk.gray(`  ${name.padEnd(8)}: Empty`));
            return;
        }
        console.log(`  ${name.padEnd(8)}: Offset 0x${ref.offset.toString(16)}, Size ${ref.size} bytes`);
        const b = new BinaryFile(buffer);
        b.seek(ref.offset);
        try {
            const page = WTPage.parse(b, ref.size, { disagg: false, skipData: false });
            if (page.success && page.pageHeader?.type === PageType.WT_PAGE_BLOCK_MANAGER) {
                let totalBlocks = 0n;
                page.extents.forEach(e => { if (!e.isMagic() && !e.isEndOfList()) totalBlocks += e.size; });
                console.log(chalk.gray(`            Total bytes in list: ${totalBlocks}`));
            }
        } catch(e) {}
    };

    printListSummary("Alloc", info.alloc);
    printListSummary("Avail", info.avail);
    printListSummary("Discard", info.discard);

    while (true) {
        const { action } = await inquirer.prompt([
            {
                type: 'list',
                name: 'action',
                message: 'Select action for this file:',
                choices: [
                    { name: 'View B-Tree root page', value: 'ROOT' },
                    { name: 'Visualize File Layout', value: 'VISUALIZE' },
                    new inquirer.Separator(),
                    { name: 'Back to file list', value: 'BACK' }
                ]
            }
        ]);

        if (action === 'BACK') break;
        if (action === 'ROOT') {
            await traverseTree(buffer, info.root.offset);
        } else if (action === 'VISUALIZE') {
            await visualizeLayout(buffer, info);
        }
    }
}

async function visualizeLayout(buffer: Buffer, info: CheckpointInfo, allocSize: number = 4096) {
    console.log(chalk.bold.cyan("\n--- Detailed File Layout Visualization ---"));
    
    const numBlocks = Math.ceil(info.fileSize / allocSize);
    const layout = new Array(numBlocks).fill({ char: '.', color: chalk.gray, type: 'Unknown' });

    const markRange = (ref: AddrRef, char: string, color: any, type: string) => {
        if (ref.size === 0) return;
        const listIdx = Math.floor(ref.offset / allocSize);
        if (listIdx < numBlocks) layout[listIdx] = { char: 'M', color: chalk.bold.yellow, type: 'Extent List Metadata' };

        const b = new BinaryFile(buffer);
        b.seek(ref.offset);
        try {
            const page = WTPage.parse(b, ref.size, { disagg: false, skipData: false, quietDisaggDetection: true });
            if (page.success && page.pageHeader?.type === PageType.WT_PAGE_BLOCK_MANAGER) {
                page.extents.forEach(e => {
                    if (!e.isMagic() && !e.isEndOfList()) {
                        const start = Math.floor(Number(e.offset) / allocSize);
                        const count = Math.ceil(Number(e.size) / allocSize);
                        for (let i = 0; i < count; i++) {
                            if (start + i < numBlocks) {
                                layout[start + i] = { char, color, type };
                            }
                        }
                    }
                });
            }
        } catch(e) {}
    };

    // Mark known lists
    markRange(info.alloc, 'A', chalk.green, 'Allocated (Recently)');
    markRange(info.avail, 'V', chalk.blue, 'Available (Free)');
    markRange(info.discard, 'D', chalk.red, 'Discarded');

    // Deep scan B-tree to find all active pages
    console.log(chalk.yellow("Scanning B-tree for active pages..."));
    const activePages = new Map<number, { type: PageType, size: number }>();
    const queue = [[info.root.offset, 4096]];
    const seen = new Set<number>();

    while (queue.length > 0) {
        const [offset, size] = queue.shift()!;
        if (seen.has(offset)) continue;
        seen.add(offset);

        const b = new BinaryFile(buffer);
        b.seek(offset);
        try {
            const page = WTPage.parse(b, size, { disagg: false, skipData: false, quietDisaggDetection: true });
            if (page.success && page.pageHeader) {
                const diskSize = (page.blockHeader as any).diskSize || 4096;
                activePages.set(offset, { type: page.pageHeader.type, size: diskSize });

                if (page.pageHeader.type === PageType.WT_PAGE_ROW_INT || page.pageHeader.type === PageType.WT_PAGE_COL_INT) {
                    page.cells.forEach(c => {
                        if (c.isAddress) {
                            const [off, sz] = decodeAddressCookie(c.data);
                            queue.push([(off + 1) * 4096, sz * 4096]);
                        }
                    });
                }
            }
        } catch(e) {}
    }

    // Mark B-tree pages in layout
    activePages.forEach((meta, offset) => {
        const start = Math.floor(offset / allocSize);
        const count = Math.ceil(meta.size / allocSize);
        let char = 'P';
        let color = chalk.bold.white;
        let typeStr = 'Page';

        if (offset === info.root.offset) {
            char = 'R'; color = chalk.bold.magenta; typeStr = 'Root Page';
        } else if (meta.type === PageType.WT_PAGE_ROW_INT || meta.type === PageType.WT_PAGE_COL_INT) {
            char = 'I'; color = chalk.bold.cyan; typeStr = 'Internal Page';
        } else if (meta.type === PageType.WT_PAGE_ROW_LEAF || meta.type === PageType.WT_PAGE_COL_VAR) {
            char = 'L'; color = chalk.white; typeStr = 'Leaf Page';
        } else if (meta.type === PageType.WT_PAGE_OVFL) {
            char = 'O'; color = chalk.magenta; typeStr = 'Overflow Page';
        }

        for (let i = 0; i < count; i++) {
            if (start + i < numBlocks) {
                layout[start + i] = { char, color, type: typeStr };
            }
        }
    });

    // Mark the File Header
    layout[0] = { char: 'H', color: chalk.bold.yellow, type: 'File Header' };

    // Calculate Stats
    const stats = {
        total: numBlocks,
        free: layout.filter(b => b.char === 'V').length,
        allocated: layout.filter(b => b.char === 'A').length,
        internal: layout.filter(b => b.char === 'I').length,
        leaf: layout.filter(b => b.char === 'L').length,
        overflow: layout.filter(b => b.char === 'O').length,
        meta: layout.filter(b => b.char === 'M').length,
        root: layout.filter(b => b.char === 'R').length,
        unknown: layout.filter(b => b.char === '.').length
    };

    const usedBlocks = stats.total - stats.free;
    const fragmentation = (stats.free / stats.total) * 100;

    console.log(chalk.bold("\nFile Statistics:"));
    console.log(`  Total Size:      ${info.fileSize} bytes (${stats.total} blocks)`);
    console.log(`  Active Data:     ${(stats.internal + stats.leaf + stats.root + stats.overflow) * allocSize} bytes`);
    console.log(`  Free space:      ${stats.free * allocSize} bytes (${fragmentation.toFixed(2)}%)`);
    console.log(`  Fragmentation:   ${fragmentation > 30 ? chalk.red("High") : fragmentation > 10 ? chalk.yellow("Moderate") : chalk.green("Low")}`);
    
    // Contiguous analysis
    let maxContigFree = 0;
    let currentContigFree = 0;
    for (const b of layout) {
        if (b.char === 'V') currentContigFree++;
        else {
            maxContigFree = Math.max(maxContigFree, currentContigFree);
            currentContigFree = 0;
        }
    }
    maxContigFree = Math.max(maxContigFree, currentContigFree);
    console.log(`  Largest Hole:    ${maxContigFree * allocSize} bytes`);

    // Print legend
    console.log(`\nLegend: ${chalk.bold.yellow('H')}=Header, ${chalk.bold.magenta('R')}=Root, ${chalk.bold.cyan('I')}=Internal, ${chalk.white('L')}=Leaf, ${chalk.magenta('O')}=Overflow, ${chalk.blue('V')}=Available(Free), ${chalk.green('A')}=Alloc, ${chalk.red('D')}=Discard, ${chalk.bold.yellow('M')}=Metadata, ${chalk.gray('.')}=Unknown`);
    console.log("");

    // Print grid
    const blocksPerLine = 60;
    for (let i = 0; i < layout.length; i += blocksPerLine) {
        const offset = (i * allocSize).toString(16).padStart(8, '0');
        const line = layout.slice(i, i + blocksPerLine).map(c => c.color(c.char)).join('');
        console.log(`0x${offset}: ${line}`);
    }
    console.log("");
    
    await inquirer.prompt([{ type: 'input', name: 'wait', message: 'Press Enter to continue...' }]);
}

async function traverseTree(buffer: Buffer, rootOffset: number) {
    let currentOffset = rootOffset;
    let history: number[] = [];
    
    while (true) {
        const b = new BinaryFile(buffer);
        b.seek(currentOffset);
        const page = WTPage.parse(b, buffer.length, { disagg: false, skipData: false });
        
        if (!page.success) {
            console.log(chalk.red(`Failed to decode page at offset 0x${currentOffset.toString(16)}.`));
            if (history.length > 0) {
                currentOffset = history.pop()!;
                continue;
            } else break;
        }

        console.log(chalk.bold.blue(`\n--- Page at Offset 0x${currentOffset.toString(16)} ---`));
        page.printPage({ verbose: true });

        const choices: any[] = [];
        if (page.pageHeader?.type === PageType.WT_PAGE_ROW_INT) {
            page.cells.forEach((c, i) => {
                if (c.isKey) {
                    const keyDesc = rawBytes(c.data);
                    const addrCell = page.cells[i+1];
                    if (addrCell && addrCell.isAddress) {
                        const [off] = decodeAddressCookie(addrCell.data);
                        choices.push({
                            name: `Key: ${keyDesc} -> Child at 0x${((off + 1) * 4096).toString(16)}`,
                            value: (off + 1) * 4096
                        });
                    }
                } else if (c.isAddress && (!page.cells[i-1] || !page.cells[i-1]!.isKey)) {
                    const [off] = decodeAddressCookie(c.data);
                    choices.push({
                        name: `Initial Child -> 0x${((off + 1) * 4096).toString(16)}`,
                        value: (off + 1) * 4096
                    });
                }
            });
        }

        const { action } = await inquirer.prompt([
            {
                type: 'list',
                name: 'action',
                message: 'Navigation:',
                choices: [
                    ...choices,
                    new inquirer.Separator(),
                    ...(history.length > 0 ? [{ name: 'Go Up', value: 'UP' }] : []),
                    { name: 'Back to file list', value: 'BACK' }
                ]
            }
        ]);

        if (action === 'BACK') break;
        if (action === 'UP') {
            currentOffset = history.pop()!;
        } else {
            history.push(currentOffset);
            currentOffset = action;
        }
    }
}
