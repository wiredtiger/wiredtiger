#!/bin/bash

rm out.log
while ! grep "read checksum error for 4096B block at offset 8192: block header checksum of 0x6c61663d doesn't match expected checksum of 0x70b44051" out.log; do
    rm dump_union_fs.*.core
    ./build/test/cppsuite/union_fs -f 2>&1 | tee out.log; ./build/test/cppsuite/union_fs 2>&1 | tee -a out.log
done

echo "==================="
echo "REPRO 0x6c61663d / 0x70b44051 mismatch! Logs in out.log"
echo "==================="
