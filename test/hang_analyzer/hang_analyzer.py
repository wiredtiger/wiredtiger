#!/usr/bin/env python
"""Hang Analyzer module.

A prototype hang analyzer for Evergreen integration to help investigate test timeouts

1. Script supports taking dumps, and/or dumping a summary of useful information about a process
2. Script will iterate through a list of interesting processes,
    and run the tools from step 1. The list of processes can be provided as an option.

Supports Linux and Windows.
"""

import csv
import glob
import itertools
import logging
import os
import platform
import re
import signal
import subprocess
import sys
import tempfile
import threading
import traceback
import time
from distutils import spawn  # pylint: disable=no-name-in-module
from io import BytesIO, TextIOWrapper
from optparse import OptionParser
_IS_WINDOWS = (sys.platform == "win32")

if _IS_WINDOWS:
    import win32event
    import win32api

"""
Helper class to read output of a subprocess.

Used to avoid deadlocks from the pipe buffer filling up and blocking the subprocess while it's
being waited on.
"""
class LoggerPipe(threading.Thread):  # pylint: disable=too-many-instance-attributes
    """Asynchronously reads the output of a subprocess and sends it to a logger."""

    # The start() and join() methods are not intended to be called directly on the LoggerPipe
    # instance. Since we override them for that effect, the super's version are preserved here.
    __start = threading.Thread.start
    __join = threading.Thread.join

    def __init__(self, logger, level, pipe_out):
        """Initialize the LoggerPipe with the specified arguments."""

        threading.Thread.__init__(self)
        # Main thread should not call join() when exiting
        self.daemon = True

        self.__logger = logger
        self.__level = level
        self.__pipe_out = pipe_out

        self.__lock = threading.Lock()
        self.__condition = threading.Condition(self.__lock)

        self.__started = False
        self.__finished = False

        LoggerPipe.__start(self)

    def start(self):
        """Start not implemented."""
        raise NotImplementedError("start should not be called directly")

    def run(self):
        """Read the output from 'pipe_out' and logs each line to 'logger'."""

        with self.__lock:
            self.__started = True
            self.__condition.notify_all()

        # Close the pipe when finished reading all of the output.
        with self.__pipe_out:
            # Avoid buffering the output from the pipe.
            for line in iter(self.__pipe_out.readline, b""):
                # Convert the output of the process from a bytestring to a UTF-8 string, and replace
                # any characters that cannot be decoded with the official Unicode replacement
                # character, U+FFFD. The log messages of MongoDB processes are not always valid
                # UTF-8 sequences. See SERVER-7506.
                line = line.decode("utf-8", "replace")
                self.__logger.log(self.__level, line.rstrip())

        with self.__lock:
            self.__finished = True
            self.__condition.notify_all()

    def join(self, timeout=None):
        """Join not implemented."""
        raise NotImplementedError("join should not be called directly")

    def wait_until_started(self):
        """Wait until started."""
        with self.__lock:
            while not self.__started:
                self.__condition.wait()

    def wait_until_finished(self):
        """Wait until finished."""
        with self.__lock:
            while not self.__finished:
                self.__condition.wait()

        # No need to pass a timeout to join() because the thread should already be done after
        # notifying us it has finished reading output from the pipe.
        LoggerPipe.__join(self)  # Tidy up the started thread.

def call(args, logger):
    """Call subprocess on args list."""
    logger.info(str(args))

    # Use a common pipe for stdout & stderr for logging.
    process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    logger_pipe = LoggerPipe(logger, logging.INFO, process.stdout)
    logger_pipe.wait_until_started()

    ret = process.wait()
    logger_pipe.wait_until_finished()

    if ret != 0:
        logger.error("Bad exit code %d", ret)
        raise Exception("Bad exit code %d from %s" % (ret, " ".join(args)))

def callo(args, logger):
    """Call subprocess on args string."""
    logger.info("%s", str(args))

    return subprocess.check_output(args)

def find_program(prog, paths):
    """Find the specified program in env PATH, or tries a set of paths."""
    loc = spawn.find_executable(prog)

    if loc is not None:
        return loc

    for loc in paths:
        full_prog = os.path.join(loc, prog)
        if os.path.exists(full_prog):
            return full_prog

    return None

def get_process_logger(debugger_output, pid, process_name):
    """Return the process logger from options specified."""
    process_logger = logging.Logger("process", level=logging.DEBUG)
    process_logger.mongo_process_filename = None

    if 'stdout' in debugger_output:
        s_handler = logging.StreamHandler(sys.stdout)
        s_handler.setFormatter(logging.Formatter(fmt="%(message)s"))
        process_logger.addHandler(s_handler)

    if 'file' in debugger_output:
        filename = "debugger_%s_%d.log" % (os.path.splitext(process_name)[0], pid)
        process_logger.mongo_process_filename = filename
        f_handler = logging.FileHandler(filename=filename, mode="w")
        f_handler.setFormatter(logging.Formatter(fmt="%(message)s"))
        process_logger.addHandler(f_handler)

    return process_logger

class WindowsDumper(object):
    """WindowsDumper class."""

    @staticmethod
    def __find_debugger(logger, debugger):
        """Find the installed debugger."""
        # We are looking for c:\Program Files (x86)\Windows Kits\8.1\Debuggers\x64
        cdb = spawn.find_executable(debugger)
        if cdb is not None:
            return cdb
        from win32com.shell import shell, shellcon

        # Cygwin via sshd does not expose the normal environment variables
        # Use the shell api to get the variable instead
        root_dir = shell.SHGetFolderPath(0, shellcon.CSIDL_PROGRAM_FILESX86, None, 0)

        for idx in range(0, 2):
            dbg_path = os.path.join(root_dir, "Windows Kits", "8." + str(idx), "Debuggers", "x64")
            logger.info("Checking for debugger in %s", dbg_path)
            if os.path.exists(dbg_path):
                return os.path.join(dbg_path, debugger)

        return None

    def dump_info(  # pylint: disable=too-many-arguments
            self, root_logger, logger, pid, process_name, take_dump):
        """Dump useful information to the console."""
        debugger = "cdb.exe"
        dbg = self.__find_debugger(root_logger, debugger)

        if dbg is None:
            root_logger.warning("Debugger %s not found, skipping dumping of %d", debugger, pid)
            return

        root_logger.info("Debugger %s, analyzing %s process with PID %d", dbg, process_name, pid)

        dump_command = ""
        if take_dump:
            # Dump to file, dump_<process name>.<pid>.mdmp
            dump_file = "dump_%s.%d.%s" % (os.path.splitext(process_name)[0], pid,
                                           self.get_dump_ext())
            dump_command = ".dump /ma %s" % dump_file
            root_logger.info("Dumping core to %s", dump_file)

        cmds = [
            ".symfix",  # Fixup symbol path
            ".symopt +0x10",  # Enable line loading (off by default in CDB, on by default in WinDBG)
            ".reload",  # Reload symbols
            "!peb",  # Dump current exe, & environment variables
            "lm",  # Dump loaded modules
            dump_command,
            "!uniqstack -pn",  # Dump All unique Threads with function arguments
            "!cs -l",  # Dump all locked critical sections
            ".detach",  # Detach
            "q"  # Quit
        ]

        call([dbg, '-c', ";".join(cmds), '-p', str(pid)], logger)

        root_logger.info("Done analyzing %s process with PID %d", process_name, pid)

    @staticmethod
    def get_dump_ext():
        """Return the dump file extension."""
        return "mdmp"

class WindowsProcessList(object):
    """WindowsProcessList class."""

    @staticmethod
    def __find_ps():
        """Find tasklist."""
        return os.path.join(os.environ["WINDIR"], "system32", "tasklist.exe")

    def dump_processes(self, logger):
        """Get list of [Pid, Process Name]."""
        ps = self.__find_ps()

        logger.info("Getting list of processes using %s", ps)

        ret = callo([ps, "/FO", "CSV"], logger)

        buff = TextIOWrapper(BytesIO(ret))
        csv_reader = csv.reader(buff)

        return [[int(row[1]), row[0]] for row in csv_reader if row[1] != "PID"]

# GDB dumper is for Linux
class GDBDumper(object):
    """GDBDumper class."""

    @staticmethod
    def __find_debugger(debugger):
        """Find the installed debugger."""
        return find_program(debugger, ['/opt/mongodbtoolchain/v3/bin/gdb', '/usr/bin'])

    def dump_info(  # pylint: disable=too-many-arguments,too-many-locals
            self, root_logger, logger, pid, process_name, take_dump):
        """Dump info."""
        debugger = "gdb"
        dbg = self.__find_debugger(debugger)

        if dbg is None:
            logger.warning("Debugger %s not found, skipping dumping of %d", debugger, pid)
            return

        root_logger.info("Debugger %s, analyzing %s process with PID %d", dbg, process_name, pid)

        dump_command = ""
        if take_dump:
            # Dump to file, dump_<process name>.<pid>.core
            dump_file = "dump_%s.%d.%s" % (process_name, pid, self.get_dump_ext())
            dump_command = "gcore %s" % dump_file
            root_logger.info("Dumping core to %s", dump_file)

        call([dbg, "--version"], logger)

        cmds = [
            "set interactive-mode off",
            "set print thread-events off",  # Suppress GDB messages of threads starting/finishing.
            "file %s" % process_name,  # Solaris must load the process to read the symbols.
            "attach %d" % pid,
            "info sharedlibrary",
            "info threads",  # Dump a simple list of commands to get the thread name
            "set python print-stack full",
            # Lock the scheduler, before running commands, which execute code in the attached process.
            "set scheduler-locking on",
            dump_command,
            "set confirm off",
            "quit",
        ]

        call([dbg, "--quiet", "--nx"] +
             list(itertools.chain.from_iterable([['-ex', b] for b in cmds])), logger)

        root_logger.info("Done analyzing %s process with PID %d", process_name, pid)

    @staticmethod
    def get_dump_ext():
        """Return the dump file extension."""
        return "core"

    @staticmethod
    def _find_gcore():
        """Find the installed gcore."""
        dbg = "/usr/bin/gcore"
        if os.path.exists(dbg):
            return dbg

        return None

class LinuxProcessList(object):
    """LinuxProcessList class."""

    @staticmethod
    def __find_ps():
        """Find ps."""
        return find_program('ps', ['/bin', '/usr/bin'])

    def dump_processes(self, logger):
        """Get list of [Pid, Process Name]."""
        ps = self.__find_ps()

        logger.info("Getting list of processes using %s", ps)

        call([ps, "--version"], logger)

        ret = callo([ps, "-eo", "pid,args"], logger)

        buff = TextIOWrapper(BytesIO(ret))
        csv_reader = csv.reader(buff, delimiter=' ', quoting=csv.QUOTE_NONE, skipinitialspace=True)

        return [[int(row[0]), os.path.split(row[1])[1]] for row in csv_reader if row[0] != "PID"]

def get_hang_analyzers():
    """Return hang analyzers."""

    dbg = None
    ps = None
    if sys.platform.startswith("linux"):
        dbg = GDBDumper()
        ps = LinuxProcessList()
    else _IS_WINDOWS or sys.platform == "cygwin":
        dbg = WindowsDumper()
        ps = WindowsProcessList()

    return [ps, dbg]

def check_dump_quota(quota, ext):
    """Check if sum of the files with ext is within the specified quota in megabytes."""

    files = glob.glob("*." + ext)

    size_sum = 0
    for file_name in files:
        size_sum += os.path.getsize(file_name)

    return size_sum <= quota

def signal_event_object(logger, pid):
    """Signal the Windows event object."""

    # Use unique event_name created.
    event_name = "Global\\Mongo_Python_" + str(pid)

    try:
        desired_access = win32event.EVENT_MODIFY_STATE
        inherit_handle = False
        task_timeout_handle = win32event.OpenEvent(desired_access, inherit_handle, event_name)
    except win32event.error as err:
        logger.info("Exception from win32event.OpenEvent with error: %s", err)
        return

    try:
        win32event.SetEvent(task_timeout_handle)
    except win32event.error as err:
        logger.info("Exception from win32event.SetEvent with error: %s", err)
    finally:
        win32api.CloseHandle(task_timeout_handle)

    logger.info("Waiting for process to report")
    time.sleep(5)

def signal_process(logger, pid, signalnum):
    """Signal process with signal, N/A on Windows."""
    try:
        os.kill(pid, signalnum)

        logger.info("Waiting for process to report")
        time.sleep(5)
    except OSError as err:
        logger.error("Hit OS error trying to signal process: %s", err)

    except AttributeError:
        logger.error("Cannot send signal to a process on Windows")

def pname_match(exact_match, pname, processes):
    """Return True if the pname matches in processes."""
    pname = os.path.splitext(pname)[0]
    for ip in processes:
        if exact_match and pname == ip or not exact_match and ip in pname:
            return True
    return False

# Basic procedure
#
# 1. Get a list of interesting processes
# 2. Dump useful information or take dumps
def main():  # pylint: disable=too-many-branches,too-many-locals,too-many-statements
    """Execute Main program."""
    root_logger = logging.Logger("hang_analyzer", level=logging.DEBUG)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(fmt="%(message)s"))
    root_logger.addHandler(handler)

    root_logger.info("Python Version: %s", sys.version)
    root_logger.info("OS: %s", platform.platform())

    try:
        if _IS_WINDOWS or sys.platform == "cygwin":
            distro = platform.win32_ver()
            root_logger.info("Windows Distribution: %s", distro)
        else:
            distro = platform.linux_distribution()
            root_logger.info("Linux Distribution: %s", distro)

    except AttributeError:
        root_logger.warning("Cannot determine Linux distro since Python is too old")

    try:
        uid = os.getuid()
        root_logger.info("Current User: %s", uid)
        current_login = os.getlogin()
        root_logger.info("Current Login: %s", current_login)
    except OSError:
        root_logger.warning("Cannot determine Unix Current Login")
    except AttributeError:
        root_logger.warning("Cannot determine Unix Current Login, not supported on Windows")

    contain_processes = ["ex_", "intpack-test", "python", "test_"]
    exact_processes = ["cursor_order", "packing-test", "t"]
    process_ids = []

    parser = OptionParser(description=__doc__)
    parser.add_option('-p', '--process-contains-names', dest='process_contains_names',
                      help='Comma separated list of process patterns to analyze')
    parser.add_option('-e', '--process-names', dest='process_exact_names',
                      help='Comma separated list of exact process names to analyze')
    parser.add_option('-d', '--process-ids', dest='process_ids', default=None,
                      help='Comma separated list of process ids (PID) to analyze, overrides -p & e')
    parser.add_option('-c', '--dump-core', dest='dump_core', action="store_true", default=False,
                      help='Dump core file for each analyzed process')
    parser.add_option('-s', '--max-core-dumps-size', dest='max_core_dumps_size', default=10000,
                      help='Maximum total size of core dumps to keep in megabytes')
    parser.add_option('-o', '--debugger-output', dest='debugger_output', action="append",
                      choices=['file', 'stdout'], default=None,
                      help="If 'stdout', then the debugger's output is written to the Python"
                      " process's stdout. If 'file', then the debugger's output is written"
                      " to a file named debugger_<process>_<pid>.log for each process it"
                      " attaches to. This option can be specified multiple times on the"
                      " command line to have the debugger's output written to multiple"
                      " locations. By default, the debugger's output is written only to the"
                      " Python process's stdout.")

    (options, _) = parser.parse_args()

    if options.debugger_output is None:
        options.debugger_output = ['stdout']

    if options.process_ids is not None:
        # process_ids is an int list of PIDs
        process_ids = [int(pid) for pid in options.process_ids.split(',')]

    if options.process_exact_names is not None:
        exact_processes = options.process_exact_names.split(',')

    if options.process_contains_names is not None:
        contain_processes = options.process_contains_names.split(',')

    [ps, dbg] = get_hang_analyzers()

    if ps is None or dbg is None:
        root_logger.warning("hang_analyzer.py: Unsupported platform: %s", sys.platform)
        exit(1)

    all_processes = ps.dump_processes(root_logger)

    # Canonicalize the process names to lowercase to handle cases where the name of the Python
    # process is /System/Library/.../Python on OS X and -p python is specified to hang_analyzer.py.
    all_processes = [(pid, process_name.lower()) for (pid, process_name) in all_processes]

    # Find all running interesting processes:
    #   If a list of process_ids is supplied, match on that.
    #   Otherwise, do a substring match on interesting_processes.
    if process_ids:
        processes = [(pid, pname) for (pid, pname) in all_processes
                     if pid in process_ids and pid != os.getpid()]

        running_pids = set([pid for (pid, pname) in all_processes])
        missing_pids = set(process_ids) - running_pids
        if missing_pids:
            root_logger.warning("The following requested process ids are not running %s",
                                list(missing_pids))
    else:
        processes = [(pid, pname) for (pid, pname) in all_processes
                     if (pname_match(True, pname, exact_processes) or pname_match(False, pname, contain_processes)) and pid != os.getpid()]

    root_logger.info("Found %d interesting processes %s", len(processes), processes)

    max_dump_size_bytes = int(options.max_core_dumps_size) * 1024 * 1024

    trapped_exceptions = []

    # Dump all processes.
    for (pid, process_name) in processes:
        process_logger = get_process_logger(options.debugger_output, pid, process_name)
        try:
            dbg.dump_info(root_logger, process_logger, pid, process_name, options.dump_core
                          and check_dump_quota(max_dump_size_bytes, dbg.get_dump_ext()))
        except Exception as err:  # pylint: disable=broad-except
            root_logger.info("Error encountered when invoking debugger %s", err)
            trapped_exceptions.append(traceback.format_exc())

    # Dump python processes by signalling them. The resmoke.py process will generate
    # the report.json, when signalled, so we do this before attaching to other processes.
    for (pid, process_name) in [(p, pn) for (p, pn) in processes if pn.startswith("python")]:
        # On Windows, we set up an event object to wait on a signal. For Cygwin, we register
        # a signal handler to wait for the signal since it supports POSIX signals.
        if _IS_WINDOWS:
            root_logger.info("Calling SetEvent to signal python process %s with PID %d",
                             process_name, pid)
            signal_event_object(root_logger, pid)
        else:
            root_logger.info("Sending signal SIGUSR1 to python process %s with PID %d", process_name, pid)
            signal_process(root_logger, pid, signal.SIGUSR1)

    root_logger.info("Done analyzing all processes for hangs")

    for exception in trapped_exceptions:
        root_logger.info(exception)
    if trapped_exceptions:
        sys.exit(1)

if __name__ == "__main__":
    main()
