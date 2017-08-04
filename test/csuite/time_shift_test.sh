#! /bin/sh

set -e

# uncomment the below line if this has to be run separately
#export TESTUTIL_ENABLE_LONG_TESTS=1

# We will run only when long tests are enabled.
test "$TESTUTIL_ENABLE_LONG_TESTS" = "1" || exit 0

export DONT_FAKE_MONOTONIC=1
RUN_OS=$(uname -s)

# linux we run with cpu affinity, to control the execution time
# if we don't control the execution time this test is not effective
echo "test read write lock for time shifting using libfaketime"
SEC1=`date +%s`
if [ "$RUN_OS" = "Darwin" ]
then
    ./test_rwlock
elif [ "$RUN_OS" = "Linux" ]
then
    taskset -c 0-1 ./test_rwlock
else
    echo "not able to decide running OS, so exiting"
    exit 1
fi

SEC2=`date +%s`
DIFF1=$((SEC2 - SEC1))

# preload libfaketime
if [ "$RUN_OS" = "Darwin" ]
then
    export DYLD_FORCE_FLAT_NAMESPACE=y
    export DYLD_INSERT_LIBRARIES=./libfaketime.1.dylib
    ./test_rwlock &
else
    LD_PRELOAD=./libfaketimeMT.so.1 taskset -c 0-1 ./test_rwlock &
fi

# get pid of test run in background
PID=$!

sleep 5s
echo "-$DIFF1""s" >| ~/.faketimerc

wait $PID

#kept echo statement here so as not to loose in cluster of test msgs. 
echo "after sleeping for 5 seconds set ~/.faketimerc value as -ve $DIFF1 seconds"
rm ~/.faketimerc

if [ "$RUN_OS" = "Darwin" ]
then
    export DYLD_FORCE_FLAT_NAMESPACE=
    export DYLD_INSERT_LIBRARIES=
fi
SEC3=`date +%s`
DIFF2=$((SEC3 - SEC2))

PERC=$((((DIFF2 - DIFF1)*100)/DIFF1)) 
echo "execution time increase in % : $PERC"
echo "normal execution time : ($DIFF1) seconds"
echo "fake time reduction by : ($DIFF1) seconds"
echo "execution time with -ve time shift : ($DIFF2) seconds"

if [ "$DIFF2" -lt "$DIFF1" ]
then
   echo "PASS"
else
   if [ "$PERC" -le 20 ]
   then
      echo "PASS"
   else
      echo "FAIL"
   fi
fi
