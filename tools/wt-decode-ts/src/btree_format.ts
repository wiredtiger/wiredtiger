import * as snappy from 'snappy';
import { BSON } from 'bson';
import crc32c from 'sse4_crc32';
import chalk from 'chalk';
import { BinaryFile, dAndH, ts, txn, unpackInt, unpack4bArray, formatTimestamp } from './binary_data.js';
import { Printer, binaryToPrettyString, rawBytes, dumpraw } from './printer.js';
import { PageStats } from './stats.js';
import { formatAddressCookie } from './ckpt_decode.js';

export enum PageType {
    WT_PAGE_INVALID = 0,
    WT_PAGE_BLOCK_MANAGER = 1,
    WT_PAGE_COL_INT = 3,
    WT_PAGE_COL_VAR = 4,
    WT_PAGE_OVFL = 5,
    WT_PAGE_ROW_INT = 6,
    WT_PAGE_ROW_LEAF = 7
}

export enum PageFlags {
    WT_PAGE_COMPRESSED = 0x01,
    WT_PAGE_EMPTY_V_ALL = 0x02,
    WT_PAGE_EMPTY_V_NONE = 0x04,
    WT_PAGE_ENCRYPTED = 0x08,
    WT_PAGE_UNUSED = 0x10,
    WT_PAGE_FT_UPDATE = 0x20
}

export class BlockFileHeader {
    static readonly WT_BLOCK_MAGIC = 120897;
    static readonly WT_BLOCK_MAJOR_VERSION = 1;
    static readonly WT_BLOCK_MINOR_VERSION = 0;

    magic: number = 0;
    major: number = 0;
    minor: number = 0;
    checksum: number = 0;
    unused: number = 0;

    static parse(b: BinaryFile): BlockFileHeader {
        const h = new BlockFileHeader();
        h.magic = b.readUint32();
        h.major = b.readUint16();
        h.minor = b.readUint16();
        h.checksum = b.readUint32();
        h.unused = b.readUint32();
        return h;
    }

    toString(): string {
        return `${chalk.bold.yellow('[Block File Header]')}
  Magic:    ${this.magic} (expected ${BlockFileHeader.WT_BLOCK_MAGIC})
  Version:  ${this.major}.${this.minor}
  Checksum: ${chalk.blue('0x' + this.checksum.toString(16))}`;
    }

    toJSON() {
        return {
            magic: this.magic,
            version: `${this.major}.${this.minor}`,
            checksum: this.checksum
        };
    }
}

export class PageHeader {
    recno: bigint = 0n;
    writeGen: bigint = 0n;
    memSize: number = 0;
    entries: number = 0;
    type: PageType = PageType.WT_PAGE_INVALID;
    flags: number = 0;
    unused: number = 0;
    version: number = 0;

    static parse(b: BinaryFile): PageHeader {
        const h = new PageHeader();
        h.recno = b.readUint64();
        h.writeGen = b.readUint64();
        h.memSize = b.readUint32();
        h.entries = b.readUint32();
        h.type = b.readUint8() as PageType;
        h.flags = b.readUint8();
        h.unused = b.readUint8();
        h.version = b.readUint8();
        return h;
    }

    toString(): string {
        const flagList: string[] = [];
        if (this.flags & PageFlags.WT_PAGE_COMPRESSED) flagList.push('COMPRESSED');
        if (this.flags & PageFlags.WT_PAGE_ENCRYPTED) flagList.push('ENCRYPTED');
        if (this.flags & PageFlags.WT_PAGE_EMPTY_V_ALL) flagList.push('EMPTY_V_ALL');
        if (this.flags & PageFlags.WT_PAGE_EMPTY_V_NONE) flagList.push('EMPTY_V_NONE');

        return `${chalk.bold.yellow('[Page Header]')}
  Type:      ${chalk.bold.magenta(PageType[this.type])} (${this.type})
  Entries:   ${chalk.cyan(this.entries)}
  MemSize:   ${chalk.cyan(this.memSize)} bytes
  WriteGen:  ${chalk.cyan(this.writeGen.toString())}
  RecNo:     ${chalk.cyan(this.recno.toString())}
  Flags:     ${chalk.blue('0x' + this.flags.toString(16))} (${flagList.join(', ') || 'None'})
  Version:   ${this.version}`;
    }

    toJSON() {
        return {
            type: PageType[this.type],
            typeId: this.type,
            entries: this.entries,
            memSize: this.memSize,
            writeGen: this.writeGen.toString(),
            recNo: this.recno.toString(),
            flags: this.flags,
            version: this.version
        };
    }
}

export enum BlockFlags {
    WT_BLOCK_DATA_CKSUM = 0x1
}

export class BlockHeader {
    diskSize: number = 0;
    checksum: number = 0;
    flags: number = 0;
    unused: number = 0;

    static parse(b: BinaryFile): BlockHeader {
        const h = new BlockHeader();
        h.diskSize = b.readUint32();
        h.checksum = b.readUint32();
        h.flags = b.readUint8();
        const unusedBuf = b.read(3);
        h.unused = unusedBuf[0]! | (unusedBuf[1]! << 8) | (unusedBuf[2]! << 16);
        return h;
    }

    toString(): string {
        return `${chalk.bold.yellow('[Block Header]')}
  DiskSize:  ${chalk.cyan(this.diskSize)} bytes
  Checksum:  ${chalk.blue('0x' + this.checksum.toString(16))}
  Flags:     ${chalk.blue('0x' + this.flags.toString(16))} (${this.flags & BlockFlags.WT_BLOCK_DATA_CKSUM ? 'DATA_CKSUM' : 'None'})`;
    }

    toJSON() {
        return {
            diskSize: this.diskSize,
            checksum: this.checksum,
            flags: this.flags
        };
    }
}

export enum BlockDisaggFlags {
    WT_BLOCK_DISAGG_DATA_CKSUM = 0x1,
    WT_BLOCK_DISAGG_ENCRYPTED = 0x2,
    WT_BLOCK_DISAGG_COMPRESSED = 0x4
}

export class BlockDisaggHeader {
    static readonly WT_BLOCK_DISAGG_MAGIC_BASE = 0xdb;
    static readonly WT_BLOCK_DISAGG_MAGIC_DELTA = 0xdd;

    magic: number = 0;
    version: number = 0;
    compatibleVersion: number = 0;
    headerSize: number = 0;
    checksum: number = 0;
    previousChecksum: number = 0;
    flags: number = 0;
    unused: number = 0;

    static parse(b: BinaryFile): BlockDisaggHeader {
        const h = new BlockDisaggHeader();
        h.magic = b.readUint8();
        h.version = b.readUint8();
        h.compatibleVersion = b.readUint8();
        h.headerSize = b.readUint8();
        h.checksum = b.readUint32();
        h.previousChecksum = b.readUint32();
        h.flags = b.readUint8();
        const unusedBuf = b.read(2);
        h.unused = unusedBuf[0]! | (unusedBuf[1]! << 8);
        return h;
    }

    toString(): string {
        const magicStr = this.magic === BlockDisaggHeader.WT_BLOCK_DISAGG_MAGIC_DELTA ? 'DELTA' : 'FULL_IMAGE';
        return `${chalk.bold.yellow('[Block Disagg Header]')}
  Magic:     ${chalk.blue('0x' + this.magic.toString(16))} (${chalk.bold.magenta(magicStr)})
  Version:   ${this.version} (compatible: ${this.compatibleVersion})
  HdrSize:   ${this.headerSize}
  Checksum:  ${chalk.blue('0x' + this.checksum.toString(16))}
  PrevCksum: ${chalk.blue('0x' + this.previousChecksum.toString(16))}
  Flags:     ${chalk.blue('0x' + this.flags.toString(16))}`;
    }

    toJSON() {
        return {
            magic: this.magic,
            magicStr: this.magic === BlockDisaggHeader.WT_BLOCK_DISAGG_MAGIC_DELTA ? 'DELTA' : 'FULL_IMAGE',
            version: this.version,
            compatibleVersion: this.compatibleVersion,
            headerSize: this.headerSize,
            checksum: this.checksum,
            previousChecksum: this.previousChecksum,
            flags: this.flags
        };
    }
}

export enum CellType {
    WT_CELL_ADDR_DEL = 0,
    WT_CELL_ADDR_INT = 1,
    WT_CELL_ADDR_LEAF = 2,
    WT_CELL_ADDR_LEAF_NO = 3,
    WT_CELL_DEL = 4,
    WT_CELL_KEY = 5,
    WT_CELL_KEY_OVFL = 6,
    WT_CELL_KEY_OVFL_RM = 12,
    WT_CELL_KEY_PFX = 7,
    WT_CELL_VALUE = 8,
    WT_CELL_VALUE_COPY = 9,
    WT_CELL_VALUE_OVFL = 10,
    WT_CELL_VALUE_OVFL_RM = 11
}

export class Cell {
    static readonly WT_CELL_KEY_SHORT = 0x01;
    static readonly WT_CELL_KEY_SHORT_PFX = 0x02;
    static readonly WT_CELL_VALUE_SHORT = 0x03;
    static readonly WT_CELL_64V = 0x04;
    static readonly WT_CELL_SECOND_DESC = 0x08;

    static readonly WT_CELL_PREPARE = 0x01;
    static readonly WT_CELL_TS_DURABLE_START = 0x02;
    static readonly WT_CELL_TS_DURABLE_STOP = 0x04;
    static readonly WT_CELL_TS_START = 0x08;
    static readonly WT_CELL_TS_STOP = 0x10;
    static readonly WT_CELL_TXN_START = 0x20;
    static readonly WT_CELL_TXN_STOP = 0x40;

    descriptor: number = 0;
    prefixCompressionCount: number = 0;
    extraDescriptor: number = 0;
    data: Buffer = Buffer.alloc(0);

    cellType?: CellType;
    prefix?: number;
    runLength?: bigint;

    isAddress: boolean = false;
    isKey: boolean = false;
    isOverflow: boolean = false;
    isShort: boolean = false;
    isUnsupported: boolean = false;
    isValue: boolean = false;
    isDelta: boolean = false;

    durableStartTs?: bigint;
    durableStopTs?: bigint;
    startTs?: bigint;
    stopTs?: bigint;
    startTxn?: bigint;
    stopTxn?: bigint;

    sizeDurableStartTs: number = 0;
    sizeDurableStopTs: number = 0;
    sizeStartTs: number = 0;
    sizeStopTs: number = 0;
    sizeStartTxn: number = 0;
    sizeStopTxn: number = 0;

    deltaFlag?: number;

    static parse(b: BinaryFile, isDelta: boolean = false, ignoreUnsupported: boolean = false): Cell {
        const cell = new Cell();
        cell.descriptor = b.readUint8();
        cell.isDelta = isDelta;

        const short = cell.descriptor & 0x3;
        let l = 0n;

        if (short === 0) {
            if (cell.descriptor & Cell.WT_CELL_SECOND_DESC) {
                cell.extraDescriptor = b.readUint8();
                cell.parseTimestamps(b);
            }

            if (cell.descriptor & Cell.WT_CELL_64V) {
                cell.runLength = b.readPackedUint64();
            }

            cell.cellType = (cell.descriptor & 0xf0) >> 4;

            if (cell.cellType === CellType.WT_CELL_VALUE) {
                if (cell.extraDescriptor !== 0) {
                    l = b.readPackedUint64();
                } else {
                    l = b.readLongLength();
                }
                if (isDelta) {
                    l = b.readPackedUint64();
                }
                cell.isValue = true;
            } else if (cell.cellType === CellType.WT_CELL_KEY) {
                l = b.readLongLength();
                cell.isKey = true;
            } else if (cell.cellType === CellType.WT_CELL_ADDR_LEAF_NO) {
                l = b.readPackedUint64();
                cell.isAddress = true;
            } else if (cell.cellType === CellType.WT_CELL_KEY_PFX) {
                cell.prefix = b.readUint8();
                l = b.readLongLength();
                cell.isKey = true;
            } else if (cell.cellType === CellType.WT_CELL_KEY_OVFL) {
                l = b.readPackedUint64();
                cell.isKey = true;
                cell.isOverflow = true;
            } else if (cell.cellType === CellType.WT_CELL_VALUE_OVFL) {
                l = b.readPackedUint64();
                cell.isOverflow = true;
                cell.isValue = true;
            } else {
                l = 0n;
                cell.isUnsupported = true;
                if (!ignoreUnsupported) {
                    throw new Error(`celltype = ${cell.cellType} not implemented`);
                }
            }
        } else if (short === Cell.WT_CELL_KEY_SHORT) {
            l = BigInt((cell.descriptor & 0xfc) >> 2);
            cell.isKey = true;
            cell.isShort = true;
        } else if (short === Cell.WT_CELL_KEY_SHORT_PFX) {
            l = BigInt((cell.descriptor & 0xfc) >> 2);
            cell.isKey = true;
            cell.isShort = true;
            cell.prefix = b.readUint8();
        } else if (short === Cell.WT_CELL_VALUE_SHORT) {
            l = BigInt((cell.descriptor & 0xfc) >> 2);
            cell.isShort = true;
            cell.isValue = true;
        }

        if (isDelta && cell.cellType === CellType.WT_CELL_VALUE) {
            cell.data = b.read(Number(l));
            cell.deltaFlag = b.readUint8();
        } else {
            cell.data = b.read(Number(l));
        }

        return cell;
    }

    private parseTimestamps(b: BinaryFile) {
        if (this.extraDescriptor === 0) return;

        if (this.extraDescriptor & Cell.WT_CELL_TS_START) {
            [this.startTs, this.sizeStartTs] = b.readPackedUint64WithSize();
        }
        if (this.extraDescriptor & Cell.WT_CELL_TXN_START) {
            [this.startTxn, this.sizeStartTxn] = b.readPackedUint64WithSize();
        }
        if (this.extraDescriptor & Cell.WT_CELL_TS_DURABLE_START) {
            [this.durableStartTs, this.sizeDurableStartTs] = b.readPackedUint64WithSize();
        }

        if (this.extraDescriptor & Cell.WT_CELL_TS_STOP) {
            [this.stopTs, this.sizeStopTs] = b.readPackedUint64WithSize();
        }
        if (this.extraDescriptor & Cell.WT_CELL_TXN_STOP) {
            [this.stopTxn, this.sizeStopTxn] = b.readPackedUint64WithSize();
        }
        if (this.extraDescriptor & Cell.WT_CELL_TS_DURABLE_STOP) {
            [this.durableStopTs, this.sizeDurableStopTs] = b.readPackedUint64WithSize();
        }

        if (this.durableStartTs !== undefined) {
            this.durableStartTs += this.startTs || 0n;
        }
        if (this.stopTs !== undefined) {
            this.stopTs += this.startTs || 0n;
        }
        if (this.stopTxn !== undefined) {
            this.stopTxn += this.startTxn || 0n;
        }
        if (this.durableStopTs !== undefined) {
            this.durableStopTs += this.stopTs || 0n;
        }
    }

    descriptorString(): string {
        let parts = [`${chalk.bold('Descriptor:')} ${chalk.blue('0x' + this.descriptor.toString(16))}`];
        if (this.extraDescriptor !== 0) {
            parts.push(`${chalk.bold('Extra:')} ${chalk.blue('0x' + this.extraDescriptor.toString(16))}`);
        }
        if (this.runLength !== undefined) {
            parts.push(`${chalk.bold('RunLength/Addr:')} ${chalk.cyan(dAndH(this.runLength))}`);
        }
        return parts.join(' | ');
    }

    typeString(): string {
        let type = 'Unknown';
        let color = chalk.white;
        if (this.isAddress) { type = 'Address'; color = chalk.yellow; }
        else if (this.isKey) { type = 'Key'; color = chalk.green; }
        else if (this.isValue) { type = 'Value'; color = chalk.blue; }
        else if (this.isUnsupported && this.cellType !== undefined) {
            type = `Unsupported (${this.cellType})`;
            color = chalk.red;
        }

        let modifiers: string[] = [];
        if (this.isOverflow) modifiers.push('Overflow');
        if (this.isShort) modifiers.push('Short');
        if (this.prefix !== undefined) modifiers.push(`Prefix=0x${this.prefix.toString(16)}`);
        
        let modStr = modifiers.length > 0 ? ` [${modifiers.join(', ')}]` : '';
        return `${chalk.bold('Type:')} ${color(type)}${chalk.gray(modStr)} | ${chalk.bold('Size:')} ${chalk.cyan(this.data.length)} bytes`;
    }

    printTimestamps(p: Printer) {
        if (this.extraDescriptor === 0) return;
        
        p.pushIndent();
        p.rintV(chalk.bold.yellow('Timestamps:'));
        p.pushIndent();
        
        if (this.extraDescriptor & Cell.WT_CELL_PREPARE) p.rintV(`${chalk.bold('Prepared:')} Yes`);
        
        if (this.startTs !== undefined) p.rintV(`${chalk.bold('Start TS:')}   ${chalk.magenta(formatTimestamp(this.startTs))}`);
        if (this.startTxn !== undefined) p.rintV(`${chalk.bold('Start Txn:')}  ${chalk.magenta(txn(this.startTxn))}`);
        if (this.durableStartTs !== undefined) p.rintV(`${chalk.bold('Durable Start TS:')} ${chalk.magenta(formatTimestamp(this.durableStartTs))}`);
        
        if (this.stopTs !== undefined) p.rintV(`${chalk.bold('Stop TS:')}    ${chalk.magenta(formatTimestamp(this.stopTs))}`);
        if (this.stopTxn !== undefined) p.rintV(`${chalk.bold('Stop Txn:')}   ${chalk.magenta(txn(this.stopTxn))}`);
        if (this.durableStopTs !== undefined) p.rintV(`${chalk.bold('Durable Stop TS:')}  ${chalk.magenta(formatTimestamp(this.durableStopTs))}`);
        
        p.popIndent();
        p.popIndent();
    }

    toJSON() {
        return {
            descriptor: this.descriptor,
            extraDescriptor: this.extraDescriptor,
            isKey: this.isKey,
            isValue: this.isValue,
            isAddress: this.isAddress,
            isOverflow: this.isOverflow,
            isShort: this.isShort,
            isDelta: this.isDelta,
            prefix: this.prefix,
            runLength: this.runLength?.toString(),
            data: this.data.toString('hex'),
            timestamps: this.extraDescriptor !== 0 ? {
                startTs: this.startTs?.toString(),
                startTxn: this.startTxn?.toString(),
                durableStartTs: this.durableStartTs?.toString(),
                stopTs: this.stopTs?.toString(),
                stopTxn: this.stopTxn?.toString(),
                durableStopTs: this.durableStopTs?.toString()
            } : null
        };
    }
}

export class ExtentItem {
    static readonly WT_BLOCK_EXTLIST_MAGIC = 71002n;
    offset: bigint = 0n;
    size: bigint = 0n;
    extraStuff: string = '';

    static parse(b: BinaryFile): ExtentItem {
        const item = new ExtentItem();
        item.offset = b.readPackedUint64();
        item.size = b.readPackedUint64();
        return item;
    }

    isMagic(): boolean {
        return this.offset === ExtentItem.WT_BLOCK_EXTLIST_MAGIC && this.size === 0n;
    }

    isEndOfList(): boolean {
        return this.offset === 0n;
    }

    toString(): string {
        return `Offset: ${this.offset}, Size: ${this.size}${this.extraStuff}`;
    }
}

export class DisaggAddr {
    version: number = 0;
    minVersion: number = 0;
    pageId: bigint = 0n;
    flags: number = 0;
    lsn: bigint = 0n;
    baseLsn: bigint = 0n;
    size: bigint = 0n;
    checksum: number = 0;

    static parse(b: Buffer): DisaggAddr {
        const addr = new DisaggAddr();
        const versionArray = unpack4bArray(b.slice(0, 1), 2);
        addr.version = versionArray[0]!;
        addr.minVersion = versionArray[1]!;

        let rest = b.slice(1) as Buffer;
        [addr.pageId, rest] = unpackInt(rest);
        let nFlags: bigint;
        [nFlags, rest] = unpackInt(rest);
        addr.flags = Number(nFlags);
        [addr.lsn, rest] = unpackInt(rest);
        [addr.baseLsn, rest] = unpackInt(rest);
        [addr.size, rest] = unpackInt(rest);
        addr.checksum = rest.readUInt32LE(0);

        return addr;
    }

    toString(): string {
        return `${chalk.bold.yellow('[Disagg Page Address]')}
  Version: ${this.version} (min: ${this.minVersion})
  PageID:  ${chalk.cyan(this.pageId.toString())}
  Flags:   ${chalk.blue('0x' + this.flags.toString(16))}
  LSN:     ${chalk.magenta(this.lsn.toString())} (base: ${this.baseLsn})
  Size:    ${chalk.cyan(this.size.toString())} bytes
  Cksum:   ${chalk.blue('0x' + this.checksum.toString(16))}`;
    }

    toJSON() {
        return {
            version: this.version,
            minVersion: this.minVersion,
            pageId: this.pageId.toString(),
            flags: this.flags,
            lsn: this.lsn.toString(),
            baseLsn: this.baseLsn?.toString(),
            size: this.size.toString(),
            checksum: this.checksum
        };
    }
}

export class WTPage {
    success: boolean = false;
    pageHeader?: PageHeader;
    blockHeader?: BlockHeader | BlockDisaggHeader;
    cells: Cell[] = [];
    extents: ExtentItem[] = [];
    rawBytes?: BinaryFile;
    offset: number = 0;

    static parse(b: BinaryFile, nbytes: number, opts: any): WTPage {
        const page = new WTPage();
        page.rawBytes = b;
        page.offset = b.tell();
        const diskPos = b.tell();

        // Peek at the potential disagg magic byte (at offset 28 from block start)
        const peekBuffer = b.slice(diskPos, diskPos + 44);
        let isDisagg = opts.disagg || false;
        
        if (!isDisagg && peekBuffer.length >= 29) {
            const magic = peekBuffer[28];
            if (magic === BlockDisaggHeader.WT_BLOCK_DISAGG_MAGIC_BASE || 
                magic === BlockDisaggHeader.WT_BLOCK_DISAGG_MAGIC_DELTA) {
                isDisagg = true;
                if (!opts.quietDisaggDetection && !opts.json) {
                    console.log(chalk.bold.cyan('Disaggregated storage format detected.'));
                    opts.quietDisaggDetection = true;
                }
            }
        }

        const headerSize = isDisagg ? 44 : 40;
        const pageData = b.read(headerSize);
        const bPage = new BinaryFile(pageData);

        page.pageHeader = PageHeader.parse(bPage);
        if (isDisagg) {
            page.blockHeader = BlockDisaggHeader.parse(bPage);
        } else {
            page.blockHeader = BlockHeader.parse(bPage);
        }

        if (page.pageHeader.unused !== 0) return page;
        if (page.pageHeader.type === PageType.WT_PAGE_INVALID) return page;

        const diskSize = isDisagg ? nbytes : (page.blockHeader as BlockHeader).diskSize;
        if (diskSize > 17 * 1024 * 1024) return page;

        // Checksum verification
        let checkSize = 0;
        if (isDisagg) {
            checkSize = (page.blockHeader!.flags & BlockDisaggFlags.WT_BLOCK_DISAGG_DATA_CKSUM) ? diskSize : 64;
        } else {
            checkSize = (page.blockHeader!.flags & BlockFlags.WT_BLOCK_DATA_CKSUM) ? diskSize : 64;
        }

        const savePos = b.tell();
        b.seek(diskPos);
        const dataForChecksum = Buffer.from(b.read(checkSize));
        b.seek(savePos);
        if (dataForChecksum.length >= 36) {
            dataForChecksum[32] = 0;
            dataForChecksum[33] = 0;
            dataForChecksum[34] = 0;
            dataForChecksum[35] = 0;
            const calculated = crc32c.calculate(dataForChecksum);
            if (calculated !== page.blockHeader!.checksum) {
                if (!opts.json) console.error(chalk.red(`? calculated checksum 0x${calculated.toString(16)} != 0x${page.blockHeader!.checksum.toString(16)}`));
                if (!opts.cont) return page;
            }
        }

        if (opts.skipData) {
            b.seek(diskPos + diskSize);
            page.success = true;
            return page;
        }

        const stats = new PageStats();
        const headerLength = b.tell() - diskPos;
        let payload: Buffer;
        if (page.pageHeader.flags & PageFlags.WT_PAGE_COMPRESSED) {
            const compressSkip = 64;
            b.read(compressSkip - headerLength); 
            const compressedLen = Number(b.readUint64());
            const compressedData = b.read(compressedLen);
            const decompressed = snappy.uncompressSync(compressedData) as Buffer;
            
            b.seek(diskPos);
            const first64 = b.read(compressSkip);
            const uncompressedPrefix = first64.slice(headerLength);
            payload = Buffer.concat([uncompressedPrefix, decompressed]);
            b.seek(diskPos + diskSize);
        } else {
            const payloadSize = Math.max(0, page.pageHeader.memSize - headerSize);
            payload = b.read(payloadSize);
            b.seek(diskPos + diskSize);
        }

        const fullPageData = Buffer.concat([pageData, payload]);
        const bFull = new BinaryFile(fullPageData);
        bFull.seek(headerSize);

        const isDelta = isDisagg && (page.blockHeader instanceof BlockDisaggHeader) && (page.blockHeader.magic === BlockDisaggHeader.WT_BLOCK_DISAGG_MAGIC_DELTA);

        if (page.pageHeader.type === PageType.WT_PAGE_ROW_INT || page.pageHeader.type === PageType.WT_PAGE_ROW_LEAF) {
            for (let i = 0; i < page.pageHeader.entries; i++) {
                const cell = Cell.parse(bFull, isDelta, true);
                page.cells.push(cell);
                stats.processTimestamps(cell);
                if (cell.isKey) {
                    stats.numKeys++;
                    stats.keysSize += cell.data.length;
                }
            }
        } else if (page.pageHeader.type === PageType.WT_PAGE_BLOCK_MANAGER) {
            page.extents = page.decodeExtList(bFull);
        }

        page.success = true;
        return page;
    }

    private decodeExtList(b: BinaryFile): ExtentItem[] {
        const extents: ExtentItem[] = [];
        let cellnum = -1;
        let lastOff = 0n;
        while (true) {
            cellnum++;
            if (b.tell() >= b.length) break;

            const extent = ExtentItem.parse(b);
            let extra = '';
            if (cellnum === 0) {
                extra = ' # magic number';
                if (!extent.isMagic()) extra += ' (INVALID)';
            }

            if (extent.isEndOfList()) {
                extra += ' # end of list';
                if (extent.size === 0n) extra += ', version 0';
                else if (extent.size === 1n) extra += ', version 1 (incomplete)';
            }

            extent.extraStuff = extra;
            extents.push(extent);

            if (extent.isEndOfList()) break;
        }
        return extents;
    }

    printPage(opts: any) {
        if (opts.json) return;
        const p = new Printer(this.rawBytes!, opts);
        
        p.separator();
        if (this.pageHeader) p.rint(this.pageHeader);
        if (this.blockHeader) p.rint(this.blockHeader);
        p.separator('-');

        if (opts.skipData) return;

        if (this.pageHeader?.type === PageType.WT_PAGE_ROW_INT || this.pageHeader?.type === PageType.WT_PAGE_ROW_LEAF) {
            p.rint(chalk.bold(`Decoding ${this.cells.length} cells:`));
            this.cells.forEach((cell, i) => {
                p.beginCell(i);
                p.rintV(cell.descriptorString());
                p.rintV(cell.typeString());
                cell.printTimestamps(p);

                p.pushIndent();
                const isDisagg = opts.disagg || (this.blockHeader instanceof BlockDisaggHeader);
                try {
                    if (cell.isValue && opts.bson) {
                        const decoded = BSON.deserialize(cell.data);
                        p.rintV(chalk.bold.yellow('BSON Data:'));
                        p.pushIndent();
                        p.rintV(JSON.stringify(decoded, null, 2));
                        p.popIndent();
                    } else if (cell.isAddress) {
                        p.rint(formatAddressCookie(cell.data, isDisagg));
                    } else {
                        p.rintV(chalk.bold.yellow('Data:'));
                        p.pushIndent();
                        p.rintV(rawBytes(cell.data));
                        p.popIndent();
                    }
                } catch (e) {
                    p.rintV(chalk.bold.red('Data (raw):'));
                    p.pushIndent();
                    p.rintV(rawBytes(cell.data));
                    p.popIndent();
                }
                p.popIndent();
                p.endCell();
            });
        } else if (this.pageHeader?.type === PageType.WT_PAGE_BLOCK_MANAGER) {
            p.rint(chalk.bold.yellow('Extent List:'));
            p.pushIndent();
            this.extents.forEach((ext, i) => {
                p.beginCell(i);
                p.rint(ext.toString());
                p.endCell();
            });
            p.popIndent();
        } else if (this.pageHeader?.type === PageType.WT_PAGE_OVFL) {
            p.rint(chalk.bold.yellow('Overflow Page Content:'));
            p.pushIndent();
            p.rint(rawBytes(this.rawBytes!.getBuffer()));
            p.popIndent();
        }
        
        p.separator();
        console.log(''); 
    }

    toJSON() {
        return {
            offset: this.offset,
            pageHeader: this.pageHeader?.toJSON(),
            blockHeader: this.blockHeader?.toJSON(),
            cells: this.cells.map(c => c.toJSON()),
            extents: this.extents.map(e => ({ offset: e.offset.toString(), size: e.size.toString(), extra: e.extraStuff }))
        };
    }
}
