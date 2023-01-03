#!/usr/bin/env python

import os, subprocess, re, sys

def print_msg(file_name, line_num, line, multiline):
    print("Illegal " + multiline + "comment in " + file_name + ":" + str(line_num - 1) + " " +
        line.strip('\n'))

def check_c_comments(file_name):
    count = 0
    with open(file_name) as f:
        line_num = 0
        for line in f:
            stripped = line.strip()
            if match(stripped):
                print_msg(file_name, line_num, line, "")
                count +=1
            line_num += 1
    return count

def match(stripped_line):
    return single_line_comment.match(stripped_line) and not url.search(stripped_line)

def validate(line, expect_match):
    if (expect_match and not match(line.strip())) or (not expect_match and match(line.strip())):
        print("Test failed:" + line)
    else:
        print("Test success")

def check_cpp_comments(file_name):
    with open(file_name) as f:
        count = 0
        line_num = 0
        length = 0
        for line in f:
            stripped = line.strip()
            if match(stripped):
                # Detecting multi-line comments of // is not easy as techincally they can occur
                # on contiguous lines without being multi-line.
                # E.g:
                # int height; // the height of our object
                # int width; // the width of our object
                # So we can't just count the number of lines we need to check that there is no text
                # preceeding the comment in subsequent lines.
                if length != 0 and not text_check.match(line.strip()):
                    # If the comment is length 2 and we found another matching line then we have
                    # found an illegal comment.
                    if length == 2:
                        print_msg(file_name, line_num, line, "multiline ")
                        count += 1
                    length += 1
                    # Try and print only one error per comment, just keep incrementing the count and
                    # the above if will only run once.
                else:
                    length += 1
            else:
                length = 0
            line_num += 1
        return count

# A collection of cases we handle.
tests = [
    (r'// hello world', True),
    (r'     // hello world', True),
    (r'hello world', False),
    (r'printf("Hello, World!\n"); // prints hello world', True),
    (r'String url = "http://www.example.com"', False),
    (r'// hello world', True),
    (r'//\\', True),
    (r'// "some comment"', True),
    (r'new URI("http://www.google.com")', False),
    (r'printf("Escaped quote\""); // Comment', True),
    (r' * http://www.google.com', False)
]

def file_is_cpp(name):
    if re.search('(.cpp|.cxx|.hpp)$', name) is not None:
        return True
    if re.search('(.c|.i|.in)$', name) is not None:
        return False

    # House style is that C++ header files use ".h", which unfortunately makes
    # this sort of code rather difficult. Luckily, libmagic can identify C/C++
    # based on content. Don't import it because Python packaging is a disaster
    # and this script needs to run reliably.
    result = subprocess.run("file {}".format(name),
                            shell=True,
                            capture_output=True,
                            text=True).stdout.strip('\n')
    return "C++" in result

# Move up to root dir.
os.chdir("..")

# We don't worry about whitespace in matches as we strip the line of whitespace.
url = re.compile(r'https?:\/\/')

# // Style comments
single_line_comment = re.compile(
    r'^(?:[^"/\\]|\"(?:[^\"\\]|\\.)*\"|/(?:[^/"\\]|\\.)|/\"(?:[^\"\\]|\\.)*\"|\\.)*//(.*)$')
# Text before comments
text_check = re.compile(r'^[^\/\n]+')

# Some directories aren't expected to comply with WiredTiger style. Ignore them.
ignore_directories=[
    'src/checksum',
    'src/os_win',
    'src/support',
]

fast=False

if len(sys.argv) > 1:
    if (sys.argv[1] == '-t'):
        for test in tests:
            validate(test[0], test[1])
        sys.exit(0)
    elif (sys.argv[1] == '-F'):
        fast=True

command = " find bench examples ext  src test -name \"*.[ch]\" -o -name \"*.in\" -o -name \
    \"*.cxx\" -o -name \"*.cpp\" -o -name \"*.i\" "
if fast:
    command = "git diff --name-only $(git merge-base --fork-point develop) bench examples ext src \
        test | grep -E '(.c|.h|.cpp|.in|.cxx|.i)$'"

result = subprocess.run(command, shell=True, capture_output=True, text=True).stdout.strip('\n')
count = 0
for file_name in result.split('\n'):
    cpp_file=False
    skip=False
    for directory in ignore_directories:
        if directory in file_name:
            skip=True
    if skip:
        continue
    if file_is_cpp(file_name):
        count += check_cpp_comments(file_name)
    else:
        count += check_c_comments(file_name)

if (count != 0):
    print('Detected ' + str(count) +' comment format issues!')

