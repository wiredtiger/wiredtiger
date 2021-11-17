#! /bin/sh

set -e

# Bypass this test for valgrind
test "$TESTUTIL_BYPASS_VALGRIND" = "1" && exit 0

# Smoke-test checkpoints as part of running "make check".

# 1. Mixed tables cases. Use four (or eight) tables because there are four table types.

echo "checkpoint: 4 mixed tables"
$TEST_WRAPPER ./t -T 4 -t m

echo "checkpoint: 8 mixed tables"
$TEST_WRAPPER ./t -T 8 -t m

echo "checkpoint: 4 mixed tables, with sweep"
$TEST_WRAPPER ./t -T 4 -t m -W 3 -r 2 -s 1 -n 100000 -k 100000

echo "checkpoint: 4 mixed tables, with timestamps"
$TEST_WRAPPER ./t -T 4 -t m -W 3 -r 2 -x -n 100000 -k 100000

# 2. FLCS cases.

echo "checkpoint: 6 fixed-length column-store tables"
$TEST_WRAPPER ./t -T 6 -t f

echo "checkpoint: 6 fixed-length column-store tables, named checkpoint"
$TEST_WRAPPER ./t -c 'TeSt' -T 6 -t f

echo "checkpoint: 6 fixed-length column-store tables with prepare"
$TEST_WRAPPER ./t -T 6 -t f -p

echo "checkpoint: 6 fixed-length column-store tables, named checkpoint with prepare"
$TEST_WRAPPER ./t -c 'TeSt' -T 6 -t f -p

echo "checkpoint: fixed-length column-store tables, stress history store. Sweep and timestamps"
$TEST_WRAPPER ./t -t c -W 3 -r 2 -D -s 1 -x -n 100000 -k 100000 -C cache_size=100MB

echo "checkpoint: fixed-length column-store tables, Sweep and timestamps"
$TEST_WRAPPER ./t -t c -W 3 -r 2 -s 1 -x -n 100000 -k 100000 -C cache_size=100MB

# 3. VLCS cases.

echo "checkpoint: 6 column-store tables"
$TEST_WRAPPER ./t -T 6 -t c

echo "checkpoint: 6 column-store tables, named checkpoint"
$TEST_WRAPPER ./t -c 'TeSt' -T 6 -t c

echo "checkpoint: 6 column-store tables with prepare"
$TEST_WRAPPER ./t -T 6 -t c -p

echo "checkpoint: 6 column-store tables, named checkpoint with prepare"
$TEST_WRAPPER ./t -c 'TeSt' -T 6 -t c -p

echo "checkpoint: column-store tables, stress history store. Sweep and timestamps"
$TEST_WRAPPER ./t -t c -W 3 -r 2 -D -s 1 -x -n 100000 -k 100000 -C cache_size=100MB

echo "checkpoint: column-store tables, Sweep and timestamps"
$TEST_WRAPPER ./t -t c -W 3 -r 2 -s 1 -x -n 100000 -k 100000 -C cache_size=100MB

# 4. Row-store cases.

echo "checkpoint: 6 row-store tables"
$TEST_WRAPPER ./t -T 6 -t r

echo "checkpoint: 6 row-store tables, named checkpoint"
$TEST_WRAPPER ./t -c 'TeSt' -T 6 -t r

echo "checkpoint: 6 row-store tables with prepare"
$TEST_WRAPPER ./t -T 6 -t r -p

echo "checkpoint: 6 row-store tables, named checkpoint with prepare"
$TEST_WRAPPER ./t -c 'TeSt' -T 6 -t r -p

echo "checkpoint: row-store tables, stress history store. Sweep and timestamps"
$TEST_WRAPPER ./t -t r -W 3 -r 2 -D -s 1 -x -n 100000 -k 100000 -C cache_size=100MB

echo "checkpoint: row-store tables, Sweep and timestamps"
$TEST_WRAPPER ./t -t r -W 3 -r 2 -s 1 -x -n 100000 -k 100000 -C cache_size=100MB

# 5. LSM cases.

echo "checkpoint: 6 LSM tables"
$TEST_WRAPPER ./t -T 6 -t l
