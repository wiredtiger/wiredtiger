#!/usr/bin/env python

import argparse
import os
import os.path
import struct



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

    if (buildTranslationMap(args.mapFileName) is False):
        print("Failed to locate or parse the map file " +
              args.mapFileName);
        print("Cannot proceed.");
        return;

if __name__ == '__main__':
    main()
