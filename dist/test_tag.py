import os
import sys

##### LOCAL VARIABLES AND CONSTANTS #####
component = ""
testingArea = ""
testType = ""

isFileTagged = False
isStart = False
isEnd = False
showMissingFiles = False

nbTests = 0

sortedTags = []
testFiles = []

taggedFiles = {}
validTags = {}

END_TAG = "[END_TAGS]"
IGNORE_FILE = "ignored_file"
NB_TAG_ARGS = 3
START_TAG = "[TEST_TAGS]"
#####

##### PROCESS ARGS #####
for arg in sys.argv:
    if arg == "-h":
        print("Usage: python test_tag.py [options]")
        print("Options:")
        print("\t-p\tShow files with no test tags")
        exit()
    elif arg == "-p":
        showMissingFiles = True
#####

##### GET ALL TEST FILES #####
for root, dirs, files in os.walk("../test/"):
    path = root.split(os.sep)
    for file in files:
        filename = os.path.join('/'.join(path), file)
        if filename.endswith("main.c") or filename.endswith(".py"):
            testFiles.append(filename)
#####

##### RETRIEVE VALID TAGS #####
validationFile = open("test_tags.ok", "r")

# The file has the following pattern
# <COMPONENT>:<TESTING_TYPE>:<TESTING_AREA>:<DESCRIPTION>
# A tag is made of the three first values: COMPONENT, TEST_TYPE and TESTING_AREA
tags = validationFile.readlines()
tags = [tag.replace('\n', '') for tag in tags]

for tag in tags:
    currentLine = tag.split(':')
    # Createa key value pair <TAG>:<DESCRIPTION>
    validTags[':'.join(currentLine[:NB_TAG_ARGS])] = ':'.join(currentLine[NB_TAG_ARGS:])

validationFile.close()
#####

##### PARSE TEST FILES #####
for filename in testFiles:
    inputFile = open(filename, "r")

    lines = inputFile.readlines()

    isStart = False
    isEnd = False
    isFileTagged = False

    # Read line by line
    for line in lines:
        # Format line
        line = line.replace('\n', '').replace('\r', '') \
                   .replace(' ', '').replace('#', '') \
                   .replace('*', '')

        # Check if line is valid
        if not line:
            continue

        # Check if end of test tag
        if END_TAG in line:
            if isStart == False:
                print("Error syntax in file " + filename + ". Unexpected tag: " + END_TAG)
                exit()
            if isFileTagged == False:
                print("Error syntax in file " + filename + ". Missing test tag.")
                exit()

            isEnd = True
            nbTests = nbTests + 1

            # Go to next file
            break

        # Check if start of test tag
        if START_TAG in line:
            if isStart == True:
                print("Error syntax in file " + filename + ". Unexpected tag: " + START_TAG)
                exit()

            isStart = True
            continue

        if isStart == True:
            # Check if file is ignored
            if line == IGNORE_FILE:
                isFileTagged = True
                print("File is ignored ! " + filename)
                continue
            # Check if current tag is valid
            if not line in validTags:
                print("Tag is not valid ! Add the new tag to test_tags.ok: \n" + line)
                exit()
            else:
                isFileTagged = True

            # Check if current tag has already matched test files
            if line in taggedFiles:
                taggedFiles[line].append(filename)
            else:
                taggedFiles[line] = [filename]

    if isFileTagged == False and showMissingFiles == True:
        print("Missing test tag in file: " + filename)

    inputFile.close()
#####

##### GENERATE OUTPUT #####
outputFile = open("../test/test_coverage.md", "w")

# Table headers
outputFile.write("|Component|Test Type|Testing Area|Description|Existing tests|" + '\n')
outputFile.write("|---|---|---|---|---|" + '\n')

# Sort tags
sortedTags = list(taggedFiles.keys())
sortedTags.sort()

for tag in sortedTags:
    # Split line
    currentLine = tag.split(":")

    # Parse tag
    component = currentLine[0]
    testType = currentLine[1]
    testingArea = currentLine[2]

    component = component.replace("_", " ").title()
    testType = testType.replace("_", " ").title()
    testingArea = testingArea.replace("_", " ").title()

    # Relative path to test file
    link = ""
    for name in taggedFiles[tag]:
        link += "[" + name + "](" + name + "), "
    link = link[:-2]

    # Write to output
    outputFile.write('|' + component + '|' + testType + '|' + \
                     testingArea + '|' + validTags[tag] + '|' \
                     + link + '\n')

outputFile.close()
#####
