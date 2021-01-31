import os

##### LOCAL VARIABLES AND CONSTANTS #####
component = ""
testingArea = ""
testType = ""

isParsingTag = False

nbTests = 0

sortedTags = []
testFiles = []

taggedFiles = {}
validTags = {}

END_TAG = "[END_TAGS]"
NB_TAG_ARGS = 3
START_TAG = "[TEST_TAGS]"
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

            if isParsingTag == False:
                print("Error syntax " + END_TAG)
                exit()

            isParsingTag = False
            nbTests = nbTests + 1

            # Go to next file
            break

        # Check if start of test tag
        if START_TAG in line:

            if isParsingTag == True:
                print("Error syntax " + START_TAG)
                exit()

            isParsingTag = True
            continue

        if isParsingTag == True:

            # Check if current tag is valid
            if not line in validTags:
                print("Tag is not valid ! Add the new tag to test_tags.ok: \n" + line)
                exit()

            # Check if current tag has already matched test files
            if line in taggedFiles:
                taggedFiles[line].append(filename)
            else:
                taggedFiles[line] = [filename]

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
