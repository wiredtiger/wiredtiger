#! /bin/bash

SCRIPT=`realpath $0`
SCRIPTPATH=`dirname $SCRIPT`

for CKPT_DELAY in $(seq 10 10 100); do
    for INSERT_WAIT in $(seq 100 100 1000); do
        CKPT_DELAY_MAX=$(echo $CKPT_DELAY + 10 | bc)
        INSERT_WAIT_MAX=$(echo $INSERT_WAIT + 100 | bc)
        echo "Running ./test_checkpoint_snapshot_race -I ${CKPT_DELAY}-${CKPT_DELAY_MAX} -C ${INSERT_WAIT}-${INSERT_WAIT_MAX}"
        ${SCRIPTPATH}/test_checkpoint_snapshot_race -I ${CKPT_DELAY}-${CKPT_DELAY_MAX} -C ${INSERT_WAIT}-${INSERT_WAIT_MAX}
    done
done