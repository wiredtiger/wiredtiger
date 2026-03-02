import * as fs from 'fs';

export function getBits(x: bigint, start: number, end: number = 0): bigint {
    return (x & ((1n << BigInt(start)) - 1n)) >> BigInt(end);
}

export function getInt(b: Buffer, size: number): bigint {
    let r = 0n;
    for (let i = 0; i < size; i++) {
        r = (r << 8n) | BigInt(b[i]);
    }
    return r;
}

export function unpackInt(b: Buffer): [bigint, Buffer] {
    const marker = b[0];
    const NEG_MULTI_MARKER = 0x10;
    const NEG_2BYTE_MARKER = 0x20;
    const NEG_1BYTE_MARKER = 0x40;
    const POS_1BYTE_MARKER = 0x80;
    const POS_2BYTE_MARKER = 0xc0;
    const POS_MULTI_MARKER = 0xe0;

    const NEG_1BYTE_MIN = -(2n ** 6n);
    const NEG_2BYTE_MIN = -(2n ** 13n) + NEG_1BYTE_MIN;
    const POS_1BYTE_MAX = 2n ** 6n - 1n;
    const POS_2BYTE_MAX = 2n ** 13n + POS_1BYTE_MAX;

    if (marker < NEG_MULTI_MARKER || marker >= 0xf0) {
        throw new Error('Not a packed integer: ' + marker.toString(16));
    } else if (marker < NEG_2BYTE_MARKER) {
        const sz = 8 - Number(getBits(BigInt(marker), 4));
        if (sz < 0) throw new Error('Not a valid packed integer');
        const part1 = (-1n << BigInt(sz << 3));
        const part2 = getInt(b.slice(1, 1 + sz), sz);
        const part3 = b.slice(sz + 1);
        return [BigInt.asIntN(64, part1 | part2), part3];
    } else if (marker < NEG_1BYTE_MARKER) {
        return [NEG_2BYTE_MIN + ((getBits(BigInt(marker), 5) << 8n) | BigInt(b[1])), b.slice(2)];
    } else if (marker < POS_1BYTE_MARKER) {
        return [NEG_1BYTE_MIN + getBits(BigInt(marker), 6), b.slice(1)];
    } else if (marker < POS_2BYTE_MARKER) {
        return [getBits(BigInt(marker), 6), b.slice(1)];
    } else if (marker < POS_MULTI_MARKER) {
        return [POS_1BYTE_MAX + 1n + ((getBits(BigInt(marker), 5) << 8n) | BigInt(b[1])), b.slice(2)];
    } else {
        const sz = Number(getBits(BigInt(marker), 4));
        return [POS_2BYTE_MAX + 1n + getInt(b.slice(1, 1 + sz), sz), b.slice(sz + 1)];
    }
}

export function unpack4bArray(data: Buffer, count: number): number[] {
    const result: number[] = [];
    let n = 0;
    let shift = 0;

    function decodeChunk(chunk: number): boolean {
        const val = chunk & 0b0111;
        let v = val;
        if (shift) {
            v = (val + 1) << shift;
        }
        n += v;
        shift += 3;
        return (chunk & 0b1000) !== 0;
    }

    for (const byte of data) {
        for (const chunk of [byte & 0b1111, byte >> 4]) {
            if (!decodeChunk(chunk)) {
                result.push(n);
                if (--count <= 0) return result;
                n = 0;
                shift = 0;
            }
        }
    }

    if (n || shift) throw new Error("Incomplete data");
    if (count > 0) throw new Error("Too many integers requested");

    return result;
}

export function decodeEscHex(s: string): Buffer {
    const b: number[] = [];
    let i = 0;
    while (i < s.length) {
        if (s[i] === '') {
            if (i + 3 > s.length) throw new Error('Not a valid escaped hex byte');
            b.push(parseInt(s[i + 1] + s[i + 2], 16));
            i += 3;
        } else {
            b.push(s.charCodeAt(i));
            i += 1;
        }
    }
    return Buffer.from(b);
}

export class BinaryFile {
    private buffer: Buffer;
    private pos: number = 0;
    private saved: Buffer = Buffer.alloc(0);

    constructor(buffer: Buffer) {
        this.buffer = buffer;
    }

    read(n: number): Buffer {
        if (n < 0) throw new Error('The read length must be >= 0');
        if (this.pos >= this.buffer.length && n > 0) {
            throw new Error(`EOF reached: attempted to read ${n} bytes at offset 0x${this.pos.toString(16)}, but buffer length is 0x${this.buffer.length.toString(16)}`);
        }
        const end = Math.min(this.pos + n, this.buffer.length);
        const result = this.buffer.slice(this.pos, end);
        if (result.length < n) {
             throw new Error(`Incomplete read: attempted to read ${n} bytes at offset 0x${this.pos.toString(16)}, but only got ${result.length} bytes before EOF (buffer length 0x${this.buffer.length.toString(16)})`);
        }
        this.pos = end;
        this.saved = Buffer.concat([this.saved, result]);
        return result;
    }

    readUint8(): number {
        return this.read(1).readUInt8(0);
    }

    readUint16(): number {
        return this.read(2).readUInt16LE(0);
    }

    readUint32(): number {
        return this.read(4).readUInt32LE(0);
    }

    readUint64(): bigint {
        return this.read(8).readBigUInt64LE(0);
    }

    readPackedUint64(): bigint {
        const [val, rest] = unpackInt(this.buffer.slice(this.pos));
        const consumed = (this.buffer.length - this.pos) - rest.length;
        this.read(consumed); // Advance pos and update saved
        return val;
    }

    readPackedUint64WithSize(): [bigint, number] {
        const start = this.pos;
        const val = this.readPackedUint64();
        return [val, this.pos - start];
    }

    readLongLength(): bigint {
        const l = this.readPackedUint64() + 64n;
        if (l < 0n) throw new Error('Negative length: ' + l);
        return l;
    }

    seek(n: number): void {
        this.saved = Buffer.alloc(0);
        this.pos = n;
    }

    tell(): number {
        return this.pos;
    }

    savedBytes(): Buffer {
        const result = this.saved;
        this.saved = Buffer.alloc(0);
        return result;
    }

    get length(): number {
        return this.buffer.length;
    }

    getBuffer(): Buffer {
        return this.buffer;
    }

    slice(start?: number, end?: number): Buffer {
        return this.buffer.slice(start, end);
    }
}

export function ts(v: bigint): string {
    return '0x' + v.toString(16);
}

export function formatTimestamp(v: bigint): string {
    return `${v.toString()} (0x${v.toString(16)})`;
}

export function txn(v: bigint): string {
    return '0x' + v.toString(16);
}

export function dAndH(n: number | bigint): string {
    return `${n} (0x${n.toString(16)})`;
}
