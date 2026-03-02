#!/usr/bin/env node
import { Command } from 'commander';
import * as fs from 'fs';
import { BinaryFile } from './binary_data.js';
import * as mdb_log_parse from './mdb_log_parse.js';
import * as file_format from './file_format.js';
import * as page_service from './page_service.js';
import * as ckpt_decode from './ckpt_decode.js';
import * as analyze from './analyze.js';

const program = new Command();

program
    .name('wt')
    .description('WiredTiger Page and Address Decoder')
    .version('1.0.0');

// Home directory analysis command
program
    .command('analyze <dir>')
    .description('Analyse a WiredTiger home directory')
    .action(async (dir) => {
        await analyze.analyzeHome(dir);
    });

// Address decoding command
program
    .command('addr <hex>')
    .description('Decode a WiredTiger address cookie (standard checkpoint, disagg checkpoint, or cell address)')
    .option('-a, --allocsize <number>', 'Allocation size for standard checkpoints', parseInt, 4096)
    .action((hex, options) => {
        ckpt_decode.decodeAddr(hex, options.allocsize);
    });

// Main decoding command (as default)
program
    .command('decode', { isDefault: true })
    .description('Decode a WiredTiger database file or log dump')
    .argument('<filename>', 'file name or "-" for stdin')
    .option('-b, --bytes', 'show bytes alongside decoding')
    .option('--bson', 'decode cell values as bson data')
    .option('--continue', 'continue on checksum failure')
    .option('-D, --debug', 'debug this tool')
    .option('--log-dump', 'input is hex dump (may be embedded in log messages)')
    .option('--disagg_table', 'input is a full disagg table (jsonl)')
    .option('-o, --offset <number>', 'seek offset before decoding', parseInt, 0)
    .option('-p, --pages <number>', 'number of pages to decode', parseInt, 0)
    .option('--raw', 'split output to also show raw bytes')
    .option('--json', 'output in machine-readable JSON format')
    .option('--skip-data', 'do not read/process data')
    .action((filename, options) => {
        let buffer: Buffer;
        if (filename === '-') {
            buffer = fs.readFileSync(0);
        } else {
            try {
                buffer = fs.readFileSync(filename);
            } catch (e: any) {
                console.error(`Error reading file: ${e.message}`);
                process.exit(1);
            }
        }

        if (options.disagg_table) {
            options.disagg = true;
            options.fragment = true;
            const lines = buffer.toString().split('\n');
            page_service.processDisaggTable(lines, options);
        } else if (options.logDump) {
            options.fragment = true;
            const blocks = mdb_log_parse.extractBlocks(buffer.toString(), options);
            for (const block of blocks) {
                file_format.wtdecodeFileObject(new BinaryFile(block), options, block.length);
            }
        } else {
            const nbytes = buffer.length;
            if (!options.json) {
                console.log(`${filename === '-' ? 'stdin' : filename}, position 0x${options.offset.toString(16)}/0x${nbytes.toString(16)}, pagelimit ${options.pages}`);
            }
            file_format.wtdecodeFileObject(new BinaryFile(buffer), options, nbytes);
        }
    });

program.parse(process.argv);
