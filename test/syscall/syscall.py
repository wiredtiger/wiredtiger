#!/usr/bin/env python
#
# Public Domain 2014-2016 MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# syscall.py
#      Command line syscall test runner
#
# For each .run file, run the corresponding program and collect strace
# output, comparing it to the contents of the .run file.
# TODO: on all systems, run trace to fd 1, may allow output interleave
# TODO: retest on Linux (with rename), and adjust output
# TODO: support 0x... or 0... to match any number
# TODO: support macros/ifdef via cpp??
# TODO: currently not doing anything with errno
# TODO: if we have multiple .run files in a test directory, how about
#       separate execution directories?
# TODO: OSX_DEFINES could be created dynamically.  We could flesh out
#       LINUX_DEFINES and using strace -x, so that the matching is more robust.

from __future__ import print_function
import argparse, fnmatch, os, platform, re, shutil, subprocess, sys

ident = r'([a-zA-Z][a-zA-Z0-9_]*)'
outputpat = re.compile(r'OUTPUT\("([^"]*)"\)')
argpat = re.compile(r'''((?:[^,"']|"[^"]*"|'[^']*')+)''')

# e.g. fd = open("blah", 0, 0);
assignpat = re.compile(ident + r'\s*=\s*' + ident + r'(\([^;]*\));')

# e.g. ASSERT_EQ(close(fd), 0);
assertpat = re.compile(r'ASSERT_([ENLG][QET])\s*\(\s*' + ident + r'\s*(\(.*\))\s*,\s*([a-zA-Z0-9_]+)\);')

# e.g. close(fd);     must return 0
callpat = re.compile(ident + r'(\(.*\));')

# e.g. open("blah", 0x0, 0x0)   = 6 0
dtruss_pat = re.compile(ident + r'(\(.*\))\s*=\s*(-*[0-9]+)\s+([-A-Za-z#0-9]*)')
dtruss_init_pat = re.compile(r'\s*SYSCALL\(args\)\s*=\s*return\s*')

strace_pat = re.compile(ident + r'(\(.*\))\s*=\s(-*[0-9]+)()')

tracepat = re.compile(r'TRACE\("([^"]*)"\)')
runpat = re.compile(r'RUN\(([^\)]*)\)')
systempat = re.compile(r'SYSTEM\("([^"]*)"\)')
# If tracepat matches, set map['trace_syscalls'] to the 0'th group, etc.
headpatterns = [ [ tracepat, 'trace_syscalls', 0],
                 [ systempat, 'required_system', 0],
                 [ runpat, 'run_args', 0] ]

# Generally, system calls must be wrapped in an ASSERT_XX() "macro".
# Exceptions are calls in this list that return 0 on success, or
# those that are hardcoded in Runner.call_compare()
calls_returning_zero = [ 'close', 'ftruncate', 'fdatasync', 'rename' ]

# A class that represents a context in which predefined constants can be
# set, and new variables can be assigned.
class VariableContext(object):
    def __getitem__(self, key):
        if key not in dir(self) or key.startswith('__'):
            raise KeyError(key)
        return getattr(self, key)

    def __setitem__(self, key, value):
        #self.__dict__[key] = value
        setattr(self, key, value)

# Encapsulate all the defines we can use in our scripts, and allow us
# to create new variables in our script via assignment.
#
class OSX_DEFINES(VariableContext):
    # OS/X's dtruss shows flag arguments as raw numbers.
    # When the .run file specifies symbolic names, they will
    # be translated using the defines here, and they will match the
    # numbers in the dtruss output.

    # From /usr/include/sys/fcntl.h:
    O_RDONLY = 0x0000
    O_WRONLY = 0x0001
    O_RDWR = 0x0002
    O_ACCMODE = 0x0003
    O_NONBLOCK = 0x0004
    O_APPEND = 0x0008
    O_SHLOCK = 0x0010
    O_EXLOCK = 0x0020
    O_ASYNC = 0x0040
    O_NOFOLLOW = 0x0100
    O_CREAT = 0x0200
    O_TRUNC = 0x0400
    O_EXCL = 0x0800
    O_CLOEXEC = 0x1000000

class LINUX_DEFINES(VariableContext):
    # Linux's strace converts flags to symbolic names.
    # When the .run file specifies symbolic names, they match lexically.
    # The downside of this is that flags in the .run file must have the
    # precise ordering used by strace.
    pass

# For hardcoded breakpoints during debugging
def bp():
    import pdb
    pdb.set_trace()

def msg(s):
    print("syscall.py: " + s, file=sys.stderr)

def simplify_path(wttop, pathname):
    wttop = os.path.join(wttop, "")
    if pathname.startswith(wttop):
        pathname = pathname[len(wttop):]
    return pathname

# A line from a file: a modified string with the file name and line number
# associated with it.
class FileLine(str):
    filename = None
    linenum = 0
    def __new__(cls, value, *args, **kwargs):
        return super(FileLine, cls).__new__(cls, value)

    def prefix(self):
        return self.filename + ':' + str(self.linenum) + ': '

class FileReader(object):
    # 'raw' means we don't ignore any lines
    def __init__(self, wttop, filename, raw = True):
        self.wttop = wttop
        self.filename = filename
        self.f = open(filename)
        self.linenum = 1
        self.raw = raw
        if not self.f:
            msg(self.filename + ': cannot open')
            sys.exit(1)

    def __enter__(self):
        if not self.f:
            return False
        return self

    def __exit__(self, typ, value, traceback):
        if self.f:
            self.f.close()
            self.f = None

    # Return True if the line is to be ignored.
    def ignore(self, line):
        if self.raw:
            return False
        return line == ''

    # strip a line of comments
    def strip_line(self, line):
        if not line:
            return None
        line = line.strip()
        if '//' in line:
            if line.startswith('//'):
                line = ''
            else:
                # This isn't exactly right, it would see "; //"
                # within a string or comment.
                m = re.match(r'^(.*;|.*\.\.\.)\s*//', line)
                if m:
                    line = m.groups()[0].strip()
        return line

    def readline(self):
        line = self.strip_line(self.f.readline())
        while line != None and self.ignore(line):
            self.linenum += 1
            line = self.strip_line(self.f.readline())
        if line:
            line = FileLine(line)
            line.filename = self.filename
            line.linenum = self.linenum
            self.linenum += 1
        else:
            line = ''     # make this somewhat compatible with file.readline
        return line

    def close(self):
        self.f.close()

class HeadOpts:
    def __init__(self):
        self.run_args = None
        self.required_system = None
        self.trace_syscalls = None

class Runner:
    def __init__(self, wttopdir, runfilename, exedir, testexe,
                 strace, args):
        if args.systype == 'Linux':
            self.descriptors = LINUX_DEFINES()
        elif args.systype == 'Darwin':
            self.descriptors = OSX_DEFINES()
        self.wttopdir = wttopdir
        self.runfilename = runfilename
        self.testexe = testexe
        self.exedir = exedir
        self.strace = strace
        self.args = args
        self.headopts = HeadOpts()
        self.dircreated = False
        outfilename = args.outfilename
        errfilename = args.errfilename
        if outfilename == None:
            self.outfilename = os.path.join(exedir, 'stdout.txt')
        else:
            self.outfilename = outfilename
        if errfilename == None:
            self.errfilename = os.path.join(exedir, 'stderr.txt')
        else:
            self.errfilename = errfilename

        self.runfile = FileReader(self.wttopdir, runfilename, False)

    def init(self, systemtype):
        # Read up until 'RUN()'
        runline = '?'
        m = None
        while runline:
            runline = self.runfile.readline()
            m = None
            for pat,attr,group in headpatterns:
                m = re.match(pat, runline)
                if m:
                    setattr(self.headopts, attr, m.groups()[group])
                    break
            if not m:
                self.fail(runline, "unknown header option: " + runline)
                return [ False, False ]
            if self.headopts.run_args != None:
                break
        if not self.headopts.trace_syscalls:
            msg("'" + self.runfile.filename + "': needs TRACE(...)")
            return [ False, False ]
        runargs = self.headopts.run_args.strip()
        if len(runargs) > 0:
            if len(runargs) < 2 or runargs[0] != '"' or runargs[-1] != '"':
                msg("'" + self.runfile.filename +
                    "': Missing double quotes in RUN arguments")
                return [ False, False ]
            runargs = runargs[1:-1]
        self.runargs = runargs.split()
        #print("SYSCALLS: " + self.headopts.trace_syscalls
        if self.headopts.required_system != None and \
            self.headopts.required_system != systemtype:
            msg("skipping '" + self.runfile.filename + "': for '" +
                self.headopts.required_system + "', this system is '"
                + systemtype + "'")
            return [ False, True ]
        return [ True, False ]

    def close(self, forcePreserve):
        self.runfile.close()
        if self.exedir and self.dircreated and \
           not self.args.preserve and not forcePreserve:
            os.chdir('..')
            shutil.rmtree(self.exedir)

    def fail(self, line, s):
        # make it work if line is None or is a plain string.
        try:
            prefix = simplify_path(self.wttopdir, line.prefix())
        except:
            prefix = 'syscall.py: '
        print(prefix + s, file=sys.stderr)

    def str_match(self, s1, s2):
        fuzzyRight = False
        if len(s1) < 2 or len(s2) < 2:
            return False
        if s1[-3:] == '...':
            fuzzyRight = True
            s1 = s1[:-3]
        if s2[-3:] == '...':
            s2 = s2[:-3]
        if s1[0] != '"' or s1[-1] != '"' or s2[0] != '"' or s2[-1] != '"':
            return False
        s1 = s1[1:-1]
        s2 = s2[1:-1]
        # We allow a trailing \0
        if s1[-2:] == '\\0':
            s1 = s1[:-2]
        if s2[-2:] == '\\0':
            s2 = s2[:-2]
        if fuzzyRight:
            return s2.startswith(s2)
        else:
            return s1 == s2

    def expr_eval(self, s):
        return eval(s, {}, self.descriptors)

    def arg_match(self, a1, a2):
        a1 = a1.strip()
        a2 = a2.strip()
        if a1 == a2:
            return True
        if len(a1) == 0 or len(a2) == 0:
            return False
        if a1[0] == '"':
            return self.str_match(a1, a2)
        #print('  arg_match: <' + a1 + '> <' + a2 + '>')
        try:
            a1value = self.expr_eval(a1)
        except Exception:
            self.fail(a1, 'unknown expression: ' + a1)
            return False
        try:
            a2value = self.expr_eval(a2)
        except Exception:
            self.fail(a2, 'unknown expression: ' + a2)
            return False
        return a1value == a2value or int(a1value) == int(a2value)

    def split_args(self, s):
        if s[0] == '(':
            s = s[1:]
        if s[-1] == ')':
            s = s[:-1]
        return argpat.split(s)[1::2]

    def args_match(self, args1, args2):
        #print('args_match: ' + str(s1) + ', ' + str(s2))
        pos = 0
        for a1 in args1:
            a1 = a1.strip()
            if a1 == '...':  # match anything?
                return True
            if pos >= len(args2):
                return False
            if not self.arg_match(a1, args2[pos]):
                return False
            pos += 1
        if pos < len(args2):
            return False
        return True

    # func(args);  is shorthand for for ASSERT_EQ(func(args), xxx);
    # where xxx may be 0 or may be derived from one of the args.
    def call_compare(self, callname, result, eargs, errline):
        if callname in calls_returning_zero:
            return self.compare("EQ", result, "0", errline)
        elif callname == 'pwrite':
            return self.compare("EQ", result, eargs[2], errline)
        else:
            self.fail(errline, 'call ' + callname +
                      ': not known, use ASSERT_EQ()')

    def compare(self, compareop, left, right, errline):
        l = self.expr_eval(left)
        r = self.expr_eval(right)
        if (compareop == "EQ" and l == r) or \
           (compareop == "NE" and l != r) or \
           (compareop == "LT" and l < r) or \
           (compareop == "LE" and l <= r) or \
           (compareop == "GT" and l > r) or \
           (compareop == "GE" and l >= r):
            return True
        else:
            self.fail(errline,
                      'call returned value: ' + left + ', comparison: (' +
                      left + ' ' + compareop + ' ' + right +
                      ') at line: ' + errline)
            return False

    def match_report(self, runline, errline, verbose, skiplines, result, desc):
        if result:
            if verbose:
                print('MATCH:')
                print('  ' + runline.prefix() + runline)
                print('    ' + errline.prefix() + errline)
        else:
            if verbose:
                if not skiplines:
                    msg('Expecting ' + desc)
                    print('  ' + runline.prefix() + runline +
                          ' does not match:')
                    print('    ' + errline.prefix() + errline)
                else:
                    print('  (... match) ' + errline.prefix() + errline)
        return result

    def match(self, runline, errline, verbose, skiplines):
        m = re.match(outputpat, runline)
        if m:
            outwant = m.groups()[0]
            return self.match_report(runline, errline, verbose, skiplines,
                                     errline == outwant, 'output line')
        if self.args.systype == 'Linux':
            em = re.match(strace_pat, errline)
        elif self.args.systype == 'Darwin':
            em = re.match(dtruss_pat, errline)
        if not em:
            self.fail(errline, 'Unknown strace/dtruss output: ' + errline)
            return False

        m = re.match(assignpat, runline)
        if m:
            if m.groups()[1] != em.groups()[0]:
                return self.match_report(runline, errline, verbose, skiplines,
                                         False, 'syscall to match assignment')

            rargs = self.split_args(m.groups()[2])
            eargs = self.split_args(em.groups()[1])
            result = self.args_match(rargs, eargs)
            if result:
                self.descriptors[m.groups()[0]] = em.groups()[2]
            return self.match_report(runline, errline, verbose, skiplines,
                                     result, 'syscall to match assignment')

        # pattern groups using example ASSERT_EQ(close(fd), 0);
        #  0   :  comparison op ("EQ")
        #  1   :  function call name "close"
        #  2   :  function call args "(fd)"
        #  3   :  comparitor "0"
        m = re.match(assertpat, runline)
        if m:
            if m.groups()[1] != em.groups()[0]:
                return self.match_report(runline, errline, verbose, skiplines,
                                         False, 'syscall to match ASSERT')

            rargs = self.split_args(m.groups()[2])
            eargs = self.split_args(em.groups()[1])
            result = self.args_match(rargs, eargs)
            if not result:
                return self.match_report(runline, errline, verbose, skiplines,
                                         result, 'syscall to match ASSERT')
            result = self.compare(m.groups()[0], em.groups()[2],
                                  m.groups()[3], errline)
            return self.match_report(runline, errline, verbose, skiplines,
                                     result, 'ASSERT')

        # A call without an enclosing ASSERT is reduced to an ASSERT,
        # depending on the particular system call.
        m = re.match(callpat, runline)
        if m:
            if m.groups()[0] != em.groups()[0]:
                return self.match_report(runline, errline, verbose, skiplines,
                                         False, 'syscall')

            rargs = self.split_args(m.groups()[1])
            eargs = self.split_args(em.groups()[1])
            result = self.args_match(rargs, eargs)
            if not result:
                return self.match_report(runline, errline, verbose, skiplines,
                                         result, 'syscall')
            result = self.call_compare(m.groups()[0], em.groups()[2],
                                     eargs, errline)
            return self.match_report(runline, errline, verbose, skiplines,
                                     result, 'syscall')

        self.fail(runline, 'unrecognized pattern in runfile:' + runline)
        return False

    def match_lines(self):
        outfile = FileReader(self.wttopdir, self.outfilename, True)
        errfile = FileReader(self.wttopdir, self.errfilename, True)

        if outfile.readline():
            self.fail(None, 'output file has content, expected to be empty')
            return False

        with outfile, errfile:
            runlines = self.order_runfile(self.runfile)
            errline = errfile.readline()
            if re.match(dtruss_init_pat, errline):
                errline = errfile.readline()
            skiplines = False
            for runline in runlines:
                if runline == '...':
                    skiplines = True
                    if self.args.verbose:
                        print('Fuzzy matching:')
                        print('  ' + runline.prefix() + runline)
                    continue
                while errline and not self.match(runline, errline,
                                                 self.args.verbose, skiplines):
                    if skiplines:
                        errline = errfile.readline()
                    else:
                        self.fail(runline, "expecting " + runline)
                        return False
                if not errline:
                    self.fail(runline, "failed to match line: " + runline)
                    return False
                errline = errfile.readline()
                if re.match(dtruss_init_pat, errline):
                    errline = errfile.readline()
                skiplines = False
            if errline and not skiplines:
                self.fail(errline, "extra lines seen starting at " + errline)
                return False
            return True

    def order_runfile(self, f):
        matchout = (self.args.systype == 'Darwin')
        out = []
        nonout = []
        s = f.readline()
        while s:
            if matchout and re.match(outputpat, s):
                out.append(s)
            else:
                nonout.append(s)
            s = f.readline()
        out.extend(nonout)
        return out

    def run(self):
        if not self.exedir:
            self.fail(None, "Execution directory not set")
            return False
        if not os.path.isfile(self.testexe):
            msg("'" + testexe + "': no such file")
            return False

        shutil.rmtree(self.exedir, ignore_errors=True)
        os.mkdir(self.exedir)
        self.dircreated = True
        os.chdir(self.exedir)

        callargs = list(self.strace)
        trace_syscalls = self.headopts.trace_syscalls
        if self.args.systype == 'Linux':
            callargs.extend(['-e', 'trace=' + trace_syscalls ])
        elif self.args.systype == 'Darwin':
            callargs.extend(['-t', trace_syscalls ])
        callargs.append(self.testexe)
        callargs.extend(self.runargs)

        outfile = open(self.outfilename, 'w')
        errfile = open(self.errfilename, 'w')
        if self.args.verbose:
            print('RUN: ' + str(callargs))
        subret = subprocess.call(callargs, stdout=outfile, stderr=errfile)
        outfile.close()
        errfile.close()
        if subret != 0:
            msg("'" + self.testexe + "': returned " + str(subret))
            return False
        return True

class SyscallCommand:
    def __init__(self, disttop):
        self.disttop = disttop

    def parse_args(self, argv):
        srcdir = os.path.join(self.disttop, 'test', 'syscall')
        self.exetopdir = os.path.join(wt_builddir, 'test', 'syscall')

        ap = argparse.ArgumentParser('Syscall test runner')
        ap.add_argument('--systype',
                        help='override system type (Linux/Windows/Darwin)')
        ap.add_argument('--errfile', dest='errfilename',
                        help='do not run the program, use this file as stderr')
        ap.add_argument('--outfile', dest='outfilename',
                        help='do not run the program, use this file as stdout')
        ap.add_argument('--preserve', action="store_true",
                        help='keep the WT_TEST.* directories')
        ap.add_argument('--verbose', action="store_true",
                        help='add some verbose information')
        ap.add_argument('tests', nargs='*',
                        help='the tests to run (defaults to all)')
        args = ap.parse_args()

        if not args.systype:
            args.systype = platform.system()   # Linux, Windows, Darwin

        self.dorun = True
        if args.errfilename or args.outfilename:
            if len(args.tests) != 1:
                msg("one test is required when --errfile or --outfile" +
                    " is specified")
                return False
            if not args.outfilename:
                args.outfilename = os.devnull
            if not args.errfilename:
                args.errfilename = os.devnull
            self.dorun = False

        # for now, we permit Linux and Darwin
        if args.systype == 'Linux':
            strace = [ 'strace' ]
        elif args.systype == 'Darwin':
            strace = [ 'sudo', os.path.join(srcdir, 'dtruss-osx.sh') ]
        else:
            msg("systype '" + args.systype + "' unsupported")
            return False
        self.args = args
        self.strace = strace
        return True

    def runone(self, runfilename, exedir, testexe, args):
        result = True
        runner = Runner(self.disttop, runfilename, exedir, testexe,
                        self.strace, args)
        okay, skip = runner.init(args.systype)
        if not okay:
            if not skip:
                result = False
        else:
            if testexe:
                print('running ' + testexe)
                if not runner.run():
                    result = False
            if result:
                print('comparing:')
                print('  ' + simplify_path(self.disttop, runfilename))
                print('  ' + simplify_path(self.disttop, runner.errfilename))
                result = runner.match_lines()
        runner.close(not result)
        if not result:
            print('************************ FAILED ************************')
            print('  see results in ' + exedir)
        print('')
        return result

    def execute(self):
        args = self.args
        result = True
        if not self.dorun:
            for testname in args.tests:
                result = self.runone(testname, None, None, args) and result
        else:
            if len(args.tests) > 0:
                tests = []
                for arg in args.tests:
                    abspath = os.path.abspath(arg)
                    tests.append([os.path.dirname(abspath), [],
                                  [os.path.basename(abspath)]])
            else:
                tests = os.walk(syscalldir)
            os.chdir(self.exetopdir)
            for path, subdirs, files in tests:
                for name in files:
                    if fnmatch.fnmatch(name, '*.run'):
                        testname = os.path.basename(os.path.normpath(path))
                        runfilename = os.path.join(path, name)
                        testexe = os.path.join(self.exetopdir,
                                               'test_' + testname)
                        exedir = os.path.join(self.exetopdir,
                                              'WT_TEST.' + testname)
                        result = self.runone(runfilename, exedir,
                                             testexe, args) and result
        return result

# Set paths
syscalldir = sys.path[0]
wt_disttop = os.path.dirname(os.path.dirname(syscalldir))

# Note: this code is borrowed from test/suite/run.py
# Check for a local build that contains the wt utility. First check in
# current working directory, then in build_posix and finally in the disttop
# directory. This isn't ideal - if a user has multiple builds in a tree we
# could pick the wrong one.
if os.path.isfile(os.path.join(os.getcwd(), 'wt')):
    wt_builddir = os.getcwd()
elif os.path.isfile(os.path.join(wt_disttop, 'wt')):
    wt_builddir = wt_disttop
elif os.path.isfile(os.path.join(wt_disttop, 'build_posix', 'wt')):
    wt_builddir = os.path.join(wt_disttop, 'build_posix')
elif os.path.isfile(os.path.join(wt_disttop, 'wt.exe')):
    wt_builddir = wt_disttop
else:
    print('Unable to find useable WiredTiger build')
    sys.exit(1)

cmd = SyscallCommand(wt_disttop)
if not cmd.parse_args(sys.argv):
    sys.exit(1)
if not cmd.execute():
    sys.exit(1)
sys.exit(0)
