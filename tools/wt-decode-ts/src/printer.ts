import chalk from 'chalk';
import { BinaryFile, dAndH, unpackInt } from './binary_data.js';

export class Printer {
    private binfile: BinaryFile;
    private isRaw: boolean;
    private skipData: boolean;
    private ext: boolean;
    private cellpfx: string = '';
    private inCell: boolean = false;
    private indentLevel: number = 0;
    private isJson: boolean = false;

    constructor(binfile: BinaryFile, opts: any) {
        this.binfile = binfile;
        this.isRaw = opts.raw || false;
        this.skipData = opts.skipData || false;
        this.ext = opts.ext || false;
        this.isJson = opts.json || false;
    }

    pushIndent() {
        this.indentLevel++;
    }

    popIndent() {
        this.indentLevel = Math.max(0, this.indentLevel - 1);
    }

    private getIndent(): string {
        return '  '.repeat(this.indentLevel);
    }

    beginCell(cellNumber: number): void {
        if (this.isJson) return;
        this.cellpfx = chalk.cyan(`${cellNumber}: `.padEnd(4));
        this.inCell = true;
        this.binfile.savedBytes(); 
    }

    endCell(): void {
        if (this.isJson) return;
        this.inCell = false;
        this.cellpfx = '';
    }

    rint(s: any): void {
        if (this.isJson) return;
        const indent = this.getIndent();
        
        if (this.isRaw) {
            let savedBytes = Buffer.from(this.binfile.savedBytes());
            if (savedBytes.length > 0) {
                let curPos = this.binfile.tell() - savedBytes.length;
                let filePos = curPos.toString(16);
                if (filePos.length > 8) {
                    filePos = '...' + filePos.slice(-5);
                } else if (filePos.length < 8) {
                    filePos = ' '.repeat(8 - filePos.length) + filePos;
                }
                filePos += ': ';

                let splitIndent = (this.cellpfx + ' '.repeat(40)).slice(0, 40);
                this.cellpfx = '';
                while (savedBytes.length > 20) {
                    console.log(chalk.gray(splitIndent + filePos + Array.from(savedBytes.slice(0, 20)).map(b => b.toString(16).padStart(2, '0')).join(' ')));
                    savedBytes = savedBytes.slice(20);
                    splitIndent = ' '.repeat(40);
                    filePos = ' '.repeat(10);
                }
                console.log(chalk.gray(splitIndent + filePos + Array.from(savedBytes).map(b => b.toString(16).padStart(2, '0')).join(' ')));
            }
        }

        let pfx = this.cellpfx;
        this.cellpfx = '';
        
        const lines = String(s).split('\n');
        for (let i = 0; i < lines.length; i++) {
            const line = lines[i]!;
            if (i === 0) {
                console.log(pfx + indent + line);
            } else {
                console.log(' '.repeat(pfx.length) + indent + line);
            }
        }
    }

    rintV(s: any): void {
        if (!this.skipData) {
            this.rint(s);
        }
    }

    rintExt(s: any): void {
        if (this.ext) {
            this.rint(s);
        }
    }

    separator(char: string = '=', length: number = 60) {
        if (this.isJson) return;
        console.log(chalk.blue(char.repeat(length)));
    }

    header(s: string) {
        if (this.isJson) return;
        console.log(chalk.bold.yellow(s));
    }
}

export function rawBytes(b: Buffer | string): string {
    if (typeof b === 'string') return b;

    let result = '';
    let s = b;
    while (s.length > 0 && s[0]! >= 0x7f) {
        try {
            const [val, rest] = unpackInt(s);
            if (result !== '') result += ' ';
            result += `<packed ${dAndH(val)}>`;
            s = rest;
        } catch (e) {
            break;
        }
    }
    if (s.length === 0) return result;

    try {
        let decoded = s.toString('utf8');
        if (/^[\x20-\x7E\n\r\t]*$/.test(decoded)) {
            if (result !== '') result += ' ';
            return `${result}"${decoded}"`;
        }
    } catch (e) {}

    return binaryToPrettyString(b, { startWithLinePrefix: false });
}

export function binaryToPrettyString(b: Buffer, opts: { perLine?: number, linePrefix?: string, startWithLinePrefix?: boolean } = {}): string {
    const perLine = opts.perLine || 16;
    const linePrefix = opts.linePrefix !== undefined ? opts.linePrefix : '';
    const startWithLinePrefix = opts.startWithLinePrefix !== undefined ? opts.startWithLinePrefix : true;

    let result = '';
    if (b.length === 0) return result;

    for (let i = 0; i < b.length; i += perLine) {
        const chunk = b.slice(i, i + perLine);
        const hex = Array.from(chunk).map(byte => byte.toString(16).padStart(2, '0')).join(' ');
        const printable = Array.from(chunk).map(byte => (byte >= 32 && byte < 127) ? String.fromCharCode(byte) : '.').join('');
        
        if (i > 0) result += '\n';
        if (startWithLinePrefix || i > 0) result += linePrefix;
        result += chalk.green(hex.padEnd(perLine * 3 - 1)) + '  ' + chalk.magenta('|' + printable + '|');
    }
    
    return result;
}

export function dumpraw(p: Printer, b: BinaryFile, pos: number): void {
    const savepos = b.tell();
    b.seek(pos);
    const data = b.read(256);
    p.rintV(chalk.red(`Raw dump at 0x${pos.toString(16)}:`));
    p.pushIndent();
    p.rintV(binaryToPrettyString(data));
    p.popIndent();
    b.seek(savepos);
}
