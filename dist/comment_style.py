#!/usr/bin/python
import os, subprocess, re, sys

def print_msg(file_name, line_num, line):
    print("Illegal comment in " + file_name + ":" + str(line_num - 1) + " " + line, end='')

def check_c_comments(file_name):
    with open(file_name) as f:
        line_num = 0
        for line in f:
            if single_line_comment.match(line):
                print_msg(file_name, line_num, line)
            line_num += 1

tests = [
    (r'// hello world', True),
    (r'     // hello world', True),
    (r'hello world', False),
    (r'System.out.println("Hello, World!\n"); // prints hello world', True),
    (r'String url = "http://www.example.com"', False),
    (r'// hello world', True),
    (r'//\\', True),
    (r'// "some comment"', True),
    (r'new URI("http://www.google.com")', False),
    (r'System.out.println("Escaped quote\""); // Comment', True),
    (r' * http://www.google.com', False)
]


def validate(line, expect_match):
    if (expect_match and not single_line_comment.match(line)) or (not expect_match and single_line_comment.match(line)):
        print("Test failed:" + line)
    else:
        print("Test success")

def check_cpp_comments(file_name):
    with open(file_name) as f:
        line_num = 0
        length = 0
        for line in f:
            if single_line_comment.match(line.strip()):
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
                        print_msg(file_name, line_num, line)
                    length += 1
                    # Try and print only one error per comment, just keep incrementing the count and the
                    # above if will only run once.
                else:
                    length += 1

            else:
                length = 0
            line_num += 1

os.chdir("..")
# // Style comments
single_line_comment = re.compile(r'^(?:[^"/\\]|\"(?:[^\"\\]|\\.)*\"|/(?:[^/"\\]|\\.)|/\"(?:[^\"\\]|\\.)*\"|\\.)*//(.*)$')
#http_match = re.compile(r'http://|https://')
# Text before comments
text_check = re.compile(r'^[^\/\n]+')
cpp_file_directories=[
'test/cppsuite',
'test/simulator',
'test/unittest',
'bench/workgen',
'ext/storage_sources',
'tools/xray_to_optrack'
]

ignore_directories=[
'src/checksum',
'src/os_win',
'src/support'
]

fast=False

if len(sys.argv) > 1:
    if (sys.argv[1] == '-t'):
        for test in tests:
            validate(test[0], test[1])
        sys.exit(0)
    elif (sys.argv[1] == '-F'):
        fast=True

result = subprocess.run(["find", "bench", "examples", "ext", "src", "test", "-name", "*.[ch]",\
   "-o", "-name", "*.in", "-o", "-name", "*.cxx", "-o", "-name", "*.cpp", "-o", "-name", "*.i"], capture_output=True, text=True).stdout.strip('\n')

for file_name in result.split('\n'):
    cpp_file=False
    skip=False
    for directory in ignore_directories:
        if directory in file_name:
            skip=True
    if skip:
        continue
    for directory in cpp_file_directories:
        if directory in file_name:
            cpp_file=True
    if cpp_file:
        pass
        #check_cpp_comments(file_name)
    else:
        check_c_comments(file_name)


