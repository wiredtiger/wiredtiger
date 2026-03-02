import { unpackInt } from './binary_data.js';
import { DisaggAddr } from './btree_format.js';
import chalk from 'chalk';
import { rawBytes } from './printer.js';

export function decodeAddr(arg: string, allocsize: number = 4096) {
    const buffer = Buffer.from(arg, 'hex');
    if (buffer.length === 0) {
        console.error(chalk.red('Invalid hex string.'));
        return;
    }

    console.log(chalk.bold.yellow(`Decoding address: ${arg}`));

    const version = buffer[0];

    if (version === 1) {
        // Standard WT Checkpoint (version 1)
        // We try to parse it as standard checkpoint first.
        let rest = buffer.slice(1);
        const result: bigint[] = [];
        while (rest.length > 0) {
            try {
                const [i, next] = unpackInt(rest as any);
                result.push(i);
                rest = next as any;
            } catch (e) {
                break;
            }
        }

        const resultLen = result.length;
        if (resultLen === 14 || resultLen === 18) {
            console.log(chalk.cyan('Detected standard WiredTiger checkpoint address.'));
            let refCnt = resultLen === 14 ? 3 : 4;
            let pos = 0;
            for (const refname of ['root', 'alloc', 'avail', 'discard']) {
                showRef(result.slice(pos, pos + refCnt), refname, allocsize);
                pos += refCnt;
            }
            showOne('file size', result[pos]!);
            showOne('checkpoint size', result[pos + 1]!);
            return;
        }
    }

    // Try regular DisaggAddr (from cells)
    try {
        const addr = DisaggAddr.parse(buffer);
        // Basic validation: checksum is the last 4 bytes
        if (buffer.length >= 5) {
            console.log(chalk.cyan('Detected regular Disaggregated address cookie.'));
            console.log(addr.toString());
            return;
        }
    } catch (e) {
        // Not a regular disagg addr
    }

    // Try Disaggregated checkpoint address
    const ints: bigint[] = [];
    let rest = buffer;
    while (rest.length > 0) {
        try {
            const [i, next] = unpackInt(rest as any);
            ints.push(i);
            rest = next as any;
        } catch (e) {
            break;
        }
    }

    if (ints.length === 5 || ints.length === 6) {
        console.log(chalk.cyan('Detected Disaggregated checkpoint address.'));
        if (ints.length === 5) {
            showOne('root page id', ints[0]!);
            showOne('root checkpoint id', ints[1]!);
            showOne('root rec id', ints[2]!);
            showOne('root size', ints[3]!);
            showOne('root checksum', ints[4]!);
        } else {
            showOne('root page id', ints[0]!);
            showOne('root lsn', ints[1]!);
            showOne('root checkpoint id', ints[2]!);
            showOne('root rec id', ints[3]!);
            showOne('root size', ints[4]!);
            showOne('root checksum', ints[5]!);
        }
    } else {
        console.error(chalk.red('Failed to decode as any known address format.'));
    }
}

export function formatAddressCookie(data: Buffer, isDisagg: boolean = false, allocSize: number = 4096): string {
    if (isDisagg) {
        try {
            const addr = DisaggAddr.parse(data);
            return addr.toString();
        } catch (e) {}
    }

    // Try standard 3-int or 4-int cookie
    try {
        const result: bigint[] = [];
        let rest = data;
        while (rest.length > 0) {
            const [i, next] = unpackInt(rest as any);
            result.push(i);
            rest = next as any;
        }

        if (result.length >= 3 && result.length <= 4) {
            let parts = [];
            let pos = 0;
            if (result.length === 4) {
                parts.push(`${chalk.bold('Object:')} ${result[pos++]}`);
            }
            const off = Number(result[pos++]);
            const size = Number(result[pos++]);
            const cksum = result[pos++];

            parts.push(`${chalk.bold('Offset:')} 0x${((off + 1) * allocSize).toString(16)}`);
            parts.push(`${chalk.bold('Size:')} ${size * allocSize}`);
            parts.push(`${chalk.bold('Cksum:')} 0x${cksum?.toString(16)}`);
            
            return `[WT Address] ${parts.join(' | ')}`;
        }
    } catch (e) {}

    // Fallback to raw bytes
    return rawBytes(data);
}

function showOne(label: string, value: bigint | number) {
    const paddedLabel = label.padEnd(20);
    console.log(`    ${paddedLabel} ${value.toString().padStart(10)}  (0x${value.toString(16)})`);
}

function showRef(ref: bigint[], name: string, allocsize: number) {
    if (ref.length === 4) {
        showOne(name + ' object', ref[0]!);
        ref = ref.slice(1);
    }
    let off = Number(ref[0]!);
    let size = Number(ref[1]!);
    let csum = ref[2]!;
    
    if (size === 0) {
        off = -1;
        csum = 0n;
    }
    
    showOne(name + ' offset', BigInt(off + 1) * BigInt(allocsize));
    showOne(name + ' size', BigInt(size) * BigInt(allocsize));
    showOne(name + ' cksum', csum);
    console.log('');
}
