#!/usr/bin/env bash

# Uncommenting the next line may be useful when debugging this script.
#set -x

usage () {
    cat << EOF
Usage: run_parallel.sh {command} {num_iter} {num_parallel}
Where:
  {command} is a string containing the command, including parameters, to run
  {num_iter} is an positive integer indicating how many iterations to execute
  {num_parallel} is an (optional) positive integer indicating how many parallel commands should be executed
     in each iteration. If not provided, the default is half the number of available CPU cores.
EOF
}

if [ "$#" -lt  2 ]; then
    echo "Illegal number of parameters."
    usage
    exit 1
fi

NCORES=$(echo "`grep -c ^processor /proc/cpuinfo` / 2" | bc)

command=$1
num_iter=$2

if [ "$#" -eq  3 ]; then
  num_parallel=$3
else
  num_parallel=$NCORES
fi

echo "run_parallel:"
echo "  number of cores: $NCORES"
echo "  command:         $command"
echo "  num_parallel:    $num_parallel"
echo "  num_iter:        $num_iter"

outf=./outfile.txt

for i in $(seq 1 $num_iter); do
  echo "Starting iteration $i" >> $outf
  echo "Starting iteration $i"

  # start the commands in parallel
  for((t=1; t<=num_parallel; t++)); do
    echo "Starting parallel command $t (of $num_parallel) in iteration $i (of $num_iter)" >> nohup.out.$t
    eval nohup $command >> nohup.out.$t 2>&1 &
  done

  # Wait for the commands to all complete
  for((t=1; t<=num_parallel; t++)); do
    wait || exit $?
  done
done
