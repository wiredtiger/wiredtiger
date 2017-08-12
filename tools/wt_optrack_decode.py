#!/usr/bin/env python

import argparse
import os
import os.path
import struct
import sys
import traceback

functionMap = {};

def buildTranslationMap(mapFileName):

    mapFile = None;

    if not os.path.exists(mapFileName):
        return False;

    try:
        mapFile = open(mapFileName, "r");
    except:
        print("Could not open " + mapFileName + " for reading");
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

    print("Processing file " + fileName);

    # Open the log file for reading
    try:
        file = open(fileName, "r");
    except:
        print("Could not open " + fileName + " for reading");
        return;

    # Open the text file for writing
    try:
        outputFile = open(fileName + ".txt", "w");
    except:
        print("Could not open file " + fileName + ".txt for writing.");
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
                print("Could not write record " + str(record) +
                      " to file " + fileName + ".txt.");
                done = True;

    print("Wrote " + str(totalRecords) + " records to " + fileName + ".txt.");
    file.close();
    outputFile.close();

def main():
    parser = argparse.ArgumentParser(description=
                                     'Convert WiredTiger operation \
                                     tracking logs from binary to \
                                     text format.');

    parser.add_argument('files', type=str, nargs='*',
                    help='optrack log files to process');

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

    # Convert the binary logs into text format.
    for fileName in args.files:
        parseFile(fileName);

if __name__ == '__main__':
    main()
