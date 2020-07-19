import os, platform
from SCons.Script import *

# OS, Architecture
os_posix = os.name == 'posix'
os_darwin = platform.system() == 'Darwin'
os_linux = platform.system() == 'Linux'
os_windows = platform.system() == 'Windows'

os_arm64 = platform.machine().startswith('arm')
os_powerpc = platform.machine().startswith('ppc')
os_x86 = platform.machine().startswith('x86')
os_zseries = platform.machine().startswith('zseries') # XXX: untested
