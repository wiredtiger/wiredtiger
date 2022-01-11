#!/bin/sh

export WIREDTIGER_CONFIG="verbose=[evict_stuck],statistics=(all),statistics_log=(wait=1,json)"

./format.sh -j 16 -F -c CONFIG_WT8356 >& out.txt
