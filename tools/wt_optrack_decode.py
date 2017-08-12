#!/usr/bin/env python2.7

import argparse
import colorsys
from multiprocessing import Process
import multiprocessing
import os
import os.path
import struct
import sys
import subprocess
import time
import traceback

class color:
   PURPLE = '\033[95m'
   CYAN = '\033[96m'
   DARKCYAN = '\033[36m'
   BLUE = '\033[94m'
   GREEN = '\033[92m'
   YELLOW = '\033[93m'
   RED = '\033[91m'
   BOLD = '\033[1m'
   UNDERLINE = '\033[4m'
   END = '\033[0m'

functionMap = {};

def buildTranslationMap(mapFileName):

    mapFile = None;

    if not os.path.exists(mapFileName):
        return False;

    try:
        mapFile = open(mapFileName, "r");
    except:
        print(color.BOLD + color.RED);
        print("Could not open " + mapFileName + " for reading");
        print(color.END);
        return;

    # Read lines from the map file and build an in-memory map
    # of translations. Each line has a function ID followed by space and
    # followed by the function name.
    #
    lines = mapFile.readlines();  # a map file is usually small

    for line in lines:

        words = line.split(" ");
        if (len(words) < 2):
            continue;

        try:
            funcID = int(words[0], 16);
        except:
            continue;

        funcName = words[1].strip();

        functionMap[funcID] = funcName;

    return True;

def funcIDtoName(funcID):

    if (functionMap.has_key(funcID)):
        return functionMap[funcID];
    else:
        return "NULL";

#
# The format of the record is written down in src/include/optrack.h
# file in the WiredTiger source tree. The current implementation assumes
# a record of three fields. The first field is the 8-byte timestamp.
# The second field is the 8-byte function ID. The third field is the
# 2-byte operation type: '0' for function entry, '1' for function exit.
# The record size would be padded to 24 bytes in the C implementation by
# the compiler, because we keep an array of records, and each new record
# has to be 8-byte aligned, since the first field has the size 8 bytes.
# So we explicitly pad the track record structure in the implementation
# to make it clear what the record size is.
#
def parseOneRecord(file):

    bytesRead = "";
    record = ();
    RECORD_SIZE = 24;

    try:
        bytesRead = file.read(RECORD_SIZE);
    except:
        return None;

    if (len(bytesRead) < RECORD_SIZE):
        return None;

    record = struct.unpack('QQhxxxxxx', bytesRead);

    return record;

def parseFile(fileName):

    done = False;
    file = None;
    outputFile = None;
    totalRecords = 0;

    print(color.BOLD + "Processing file " + fileName + color.END);

    # Open the log file for reading
    try:
        file = open(fileName, "r");
    except:
        print(color.BOLD + color.RED);
        print("Could not open " + fileName + " for reading");
        print(color.END);
        return;

    # Open the text file for writing
    try:
        outputFile = open(fileName + ".txt", "w");
    except:
        print(color.BOLD + color.RED);
        print("Could not open file " + fileName + ".txt for writing.");
        print(color.END);
        return;

    while (not done):
        record = parseOneRecord(file);

        if ((record is None) or len(record) < 3):
            done = True;
        else:
            try:
                time = record[0];
                funcName = funcIDtoName(record[1]);
                opType = record[2];

                outputFile.write(str(opType) + " " + funcName + " " + str(time)
                                 + "\n");
                totalRecords += 1;
            except:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                traceback.print_exception(exc_type, exc_value, exc_traceback);
                print(color.BOLD + color.RED);
                print("Could not write record " + str(record) +
                      " to file " + fileName + ".txt.");
                print(color.END);
                done = True;

    print("Wrote " + str(totalRecords) + " records to " + fileName + ".txt.");
    file.close();
    outputFile.close();

def waitOnOneProcess(runningProcesses):

    success = False;
    for fname, p in runningProcesses.items():
        if (not p.is_alive()):
            del runningProcesses[fname];
            success = True;

    # If we have not found a terminated process, sleep for a while
    if (not success):
        time.sleep(5);

def main():

    runnableProcesses = {};
    returnValues = {};
    spawnedProcesses = {};
    successfullyProcessedFiles = [];
    targetParallelism = multiprocessing.cpu_count();
    terminatedProcesses = {};

    parser = argparse.ArgumentParser(description=
                                     'Convert WiredTiger operation \
                                     tracking logs from binary to \
                                     text format.');

    parser.add_argument('files', type=str, nargs='*',
                    help='optrack log files to process');

    parser.add_argument('-j', dest='jobParallelism', type=int,
                        default='0');

    parser.add_argument('-m', '--mapfile', dest='mapFileName', type=str,
                        default='optrack-map.txt');

    args = parser.parse_args();

    print("Running with the following parameters:");
    for key, value in vars(args).items():
        print ("\t" + key + ": " + str(value));

    # Parse the map of function ID to name translations.
    if (buildTranslationMap(args.mapFileName) is False):
        print("Failed to locate or parse the map file " +
              args.mapFileName);
        print("Cannot proceed.");
        return;

    # Determine the target job parallelism
    if (args.jobParallelism > 0):
        targetParallelism = args.jobParallelism;
    if (targetParallelism == 0):
        targetParallelism = len(args.files);
    print(color.BLUE + color.BOLD);
    print("Will process " + str(targetParallelism) + " files in parallel.");
    print(color.END);

    # Prepare the processes that will parse files, one per file
    if (len(args.files) > 0):
        for fname in args.files:
            p = Process(target=parseFile, args=(fname,));
            runnableProcesses[fname] = p;

    # Spawn these processes, not exceeding the desired parallelism
    while (len(runnableProcesses) > 0):
        while (len(spawnedProcesses) < targetParallelism
               and len(runnableProcesses) > 0):

            fname, p = runnableProcesses.popitem();
            p.start();
            spawnedProcesses[fname] = p;

        # Find at least one terminated process
        waitOnOneProcess(spawnedProcesses);

    # Wait for all processes to terminate
    while (len(spawnedProcesses) > 0):
        waitOnOneProcess(spawnedProcesses);

if __name__ == '__main__':
    main()
