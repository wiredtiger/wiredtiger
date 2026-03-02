import chalk from 'chalk';
import { BinaryFile } from './binary_data.js';
import { WTPage, DisaggAddr } from './btree_format.js';
import { Printer } from './printer.js';

export class PageLogMetadata {
    lsn: bigint;
    pageId: bigint;
    tableId: number;
    baseLsn?: bigint;
    backlinkLsn?: bigint;
    flags: number;

    constructor(logEntry: any) {
        this.lsn = BigInt(logEntry.lsn?.lsn || 0);
        const meta = logEntry.metadata || {};
        this.pageId = BigInt(this.extractValueRequired(meta, "page_id"));
        this.tableId = this.extractValueRequired(meta, "table_id");
        this.flags = this.extractValueRequired(meta, "flags");
        this.baseLsn = this.extractValue(meta, "base_lsn");
        this.backlinkLsn = this.extractValue(meta, "backlink_lsn");
    }

    private extractValue(metadata: any, key: string): bigint | undefined {
        if (!(key in metadata)) return undefined;
        const valWrapper = metadata[key]?.val;
        if (valWrapper) {
            const keys = Object.keys(valWrapper);
            if (keys.length > 0) return BigInt(valWrapper[keys[0]!]);
        }
        return undefined;
    }

    private extractValueRequired(metadata: any, key: string): any {
        const val = this.extractValue(metadata, key);
        if (val === undefined) throw new Error(`Missing ${key} in metadata`);
        return typeof val === 'bigint' ? Number(val) : val;
    }

    isDelta(): boolean {
        return this.flags === 0; // UPDATE_TYPE_DELTA
    }

    isMetadataPage(): boolean {
        return this.tableId === 1;
    }

    toString(): string {
        return `${chalk.bold.yellow('[Disagg Page Metadata]')}
  PageID:      ${chalk.cyan(this.pageId.toString())}
  TableID:     ${chalk.cyan(this.tableId.toString())}
  LSN:         ${chalk.magenta(this.lsn.toString())}
  BaseLSN:     ${chalk.magenta(this.baseLsn?.toString() ?? 'None')}
  BacklinkLSN: ${chalk.magenta(this.backlinkLsn?.toString() ?? 'None')}`;
    }

    toJSON() {
        return {
            lsn: this.lsn.toString(),
            pageId: this.pageId.toString(),
            tableId: this.tableId,
            flags: this.flags,
            baseLsn: this.baseLsn?.toString(),
            backlinkLsn: this.backlinkLsn?.toString()
        };
    }
}

export function processDisaggTable(lines: string[], opts: any): void {
    const collectedData: any[] = [];

    for (const line of lines) {
        if (!line.trim()) continue;
        const pages = JSON.parse(line);
        const entries = pages.entries || [];

        for (const entry of entries) {
            const meta = new PageLogMetadata(entry);
            if (!entry.entry || entry.entry.length === 0) continue;

            const entryBytes = Buffer.from(entry.entry);
            const b = new BinaryFile(entryBytes);
            const p = new Printer(b, opts);

            if (!opts.json) {
                p.rint(meta.toString());
            }

            if (meta.isMetadataPage()) {
                if (opts.json) {
                    const pageString = entryBytes.toString('ascii');
                    const addrMatch = pageString.match(/addr="([^"]+)"/);
                    let rootPageAddr = null;
                    if (addrMatch) {
                        rootPageAddr = DisaggAddr.parse(Buffer.from(addrMatch[1]!, 'hex')).toJSON();
                    }
                    collectedData.push({
                        metadata: meta.toJSON(),
                        type: 'disagg_metadata',
                        content: pageString,
                        rootPageAddr
                    });
                } else {
                    p.rint(chalk.bold.yellow('Disagg Metadata File:'));
                    const pageString = entryBytes.toString('ascii');
                    p.rint(`  ${pageString}`);
                    const addrMatch = pageString.match(/addr="([^"]+)"/);
                    if (addrMatch) {
                        const addr = DisaggAddr.parse(Buffer.from(addrMatch[1]!, 'hex'));
                        p.rint(chalk.bold.yellow('Metadata Table Root Page:'));
                        p.rint(addr.toString());
                    }
                    p.rint('');
                }
                continue;
            }

            const page = WTPage.parse(b, entryBytes.length, opts);
            if (opts.json) {
                collectedData.push({
                    metadata: meta.toJSON(),
                    page: page.toJSON()
                });
            } else {
                page.printPage(opts);
                p.rint('');
            }
        }
    }

    if (opts.json) {
        console.log(JSON.stringify(collectedData, null, 2));
    }
}
