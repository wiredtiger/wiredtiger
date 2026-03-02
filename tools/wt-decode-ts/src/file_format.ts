import cliProgress from 'cli-progress';
import chalk from 'chalk';
import { BinaryFile, dAndH } from './binary_data.js';
import { BlockFileHeader, WTPage } from './btree_format.js';
import { Printer } from './printer.js';

export function fileHeaderDecode(p: Printer, b: BinaryFile): void {
    const h = BlockFileHeader.parse(b);
    p.rint('magic: ' + h.magic);
    p.rint('major: ' + h.major);
    p.rint('minor: ' + h.minor);
    p.rint('checksum: ' + h.checksum);
    if (h.magic !== BlockFileHeader.WT_BLOCK_MAGIC) {
        p.rint(chalk.red('bad magic number'));
        return;
    }
    if (h.major !== BlockFileHeader.WT_BLOCK_MAJOR_VERSION) {
        p.rint(chalk.red('bad major number'));
        return;
    }
    if (h.minor !== BlockFileHeader.WT_BLOCK_MINOR_VERSION) {
        p.rint(chalk.red('bad minor number'));
        return;
    }
    p.rint('');
}

export function wtdecodeFileObject(b: BinaryFile, opts: any, nbytes: number): void {
    const p = new Printer(b, opts);
    let pagecount = 0;
    let startblock = opts.offset || 0;

    const collectedPages: any[] = [];

    if (startblock === 0 && !opts.fragment) {
        // Peek at magic
        const buffer = b.getBuffer();
        if (buffer.length >= 4) {
            const magic = buffer.readUInt32LE(0);
            if (magic === BlockFileHeader.WT_BLOCK_MAGIC) {
                fileHeaderDecode(p, b);
                startblock = (b.tell() + 0x1ff) & ~(0x1FF);
            } else {
                if (!opts.json) {
                    console.log(chalk.yellow('No WT file header detected, treating as fragment.'));
                }
            }
        }
    }

    const progressBar = (!opts.json && !opts.logDump && nbytes > 0 && process.stdout.isTTY) ? new cliProgress.SingleBar({
        format: 'Decoding |' + chalk.cyan('{bar}') + '| {percentage}% || {value}/{total} Bytes || Speed: {speed}',
        barCompleteChar: '\u2588',
        barIncompleteChar: '\u2591',
        hideCursor: true
    }, cliProgress.Presets.shades_classic) : null;

    if (progressBar) progressBar.start(nbytes, startblock);

    while ((nbytes === 0 || startblock < nbytes) && (opts.pages === 0 || pagecount < opts.pages)) {
        if (!opts.json && !progressBar) {
            const d_h = dAndH(startblock);
            console.log(chalk.bold.blue('Decode at ' + d_h));
        }
        
        b.seek(startblock);
        try {
            const page = WTPage.parse(b, nbytes, opts);
            if (page.success) {
                if (opts.json) {
                    collectedPages.push(page.toJSON());
                } else {
                    page.printPage(opts);
                }
            }
            if (!opts.json) p.rint('');
        } catch (e: any) {
            if (!opts.json) p.rint(chalk.red(`ERROR decoding block at ${dAndH(startblock)}: ${e.message}`));
            if (opts.debug) console.error(e);
        }
        
        let pos = b.tell();
        pos = (pos + 0x1FF) & ~(0x1FF);

        if (startblock === pos) {
            startblock += 0x200;
        } else {
            startblock = pos;
        }
        
        if (progressBar) {
            progressBar.update(Math.min(startblock, nbytes));
        }
        
        pagecount++;
    }

    if (progressBar) progressBar.stop();

    if (opts.json) {
        console.log(JSON.stringify(collectedPages, null, 2));
    } else {
        p.rint('');
    }
}
