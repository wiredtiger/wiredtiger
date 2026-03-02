export class PageStats {
    numKeys: number = 0;
    keysSize: number = 0;

    numDStartTs: number = 0;
    dStartTsSize: number = 0;
    numDStopTs: number = 0;
    dStopTsSize: number = 0;

    numStartTs: number = 0;
    startTsSize: number = 0;
    numStopTs: number = 0;
    stopTsSize: number = 0;
    numStartTxn: number = 0;
    startTxnSize: number = 0;
    numStopTxn: number = 0;
    stopTxnSize: number = 0;

    get numTs(): number {
        return this.numDStartTs + this.numDStopTs + this.numStartTs + this.numStopTs;
    }

    get tsSize(): number {
        return this.dStartTsSize + this.dStopTsSize + this.startTsSize + this.stopTsSize;
    }

    get numTxn(): number {
        return this.numStartTxn + this.numStopTxn;
    }

    get txnSize(): number {
        return this.startTxnSize + this.stopTxnSize;
    }

    processTimestamps(cell: any): void {
        if (!cell.extraDescriptor) return;

        if (cell.startTs !== undefined) {
            this.startTsSize += cell.sizeStartTs || 0;
            this.numStartTs++;
        }
        if (cell.startTxn !== undefined) {
            this.startTxnSize += cell.sizeStartTxn || 0;
            this.numStartTxn++;
        }
        if (cell.durableStartTs !== undefined) {
            this.dStartTsSize += cell.sizeDurableStartTs || 0;
            this.numDStartTs++;
        }

        if (cell.stopTs !== undefined) {
            this.stopTsSize += cell.sizeStopTs || 0;
            this.numStopTs++;
        }
        if (cell.stopTxn !== undefined) {
            this.stopTxnSize += cell.sizeStopTxn || 0;
            this.numStopTxn++;
        }
        if (cell.durableStopTs !== undefined) {
            this.dStopTsSize += cell.sizeDurableStopTs || 0;
            this.numDStopTs++;
        }
    }

    static csvCols(): string[] {
        return [
            'num keys', 'keys size',
            'num durable start ts', 'durable start ts size',
            'num durable stop ts', 'durable stop ts size',
            'num start ts', 'start ts size',
            'num stop ts', 'stop ts size',
            'num ts', 'ts size',
            'num start txn', 'start txn size',
            'num stop txn', 'stop txn size',
            'num txn', 'txn size'
        ];
    }

    toCsvCols(): (number | string)[] {
        return [
            this.numKeys, this.keysSize,
            this.numDStartTs, this.dStartTsSize,
            this.numDStopTs, this.dStopTsSize,
            this.numStartTs, this.startTsSize,
            this.numStopTs, this.stopTsSize,
            this.numTs, this.tsSize,
            this.numStartTxn, this.startTxnSize,
            this.numStopTxn, this.stopTxnSize,
            this.numTxn, this.txnSize,
        ];
    }
}
