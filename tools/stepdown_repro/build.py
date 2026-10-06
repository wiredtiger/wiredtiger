#!/usr/bin/env python3
#
# Copyright (c) 2014-present MongoDB, Inc.
# Copyright (c) 2008-2014 WiredTiger, Inc.
#    All rights reserved.
#
# See the file LICENSE for redistribution information.

"""Compile the driver against a configured Debug or ASan build."""

import argparse
import hashlib
import json
import pathlib
import platform
import re
import shlex
import subprocess


root = pathlib.Path(__file__).resolve().parents[2]
parser = argparse.ArgumentParser()
parser.add_argument('--build', default='build')
parser.add_argument('--output', default='tools/stepdown_repro/stepdown-repro')
args = parser.parse_args()
build = (root / args.build).resolve()
cache = {}
for line in (build / 'CMakeCache.txt').read_text().splitlines():
    if '=' in line and not line.startswith(('#', '//')):
        name, value = line.split('=', 1)
        cache[name.split(':', 1)[0]] = value
compiler = cache['CMAKE_C_COMPILER']
def require(condition, message):
    if not condition:
        parser.error(message)

require(cache['CMAKE_BUILD_TYPE'] in ('Debug', 'ASan'), 'only Debug and Clang ASan are supported')
for option in ('ENABLE_SHARED', 'ENABLE_PALITE'):
    require(cache.get(option, '').upper() in ('ON', 'TRUE', '1', 'YES'), f'{option}=ON is required')
configuration = (build / 'config/wiredtiger_config.h').read_text()
require(bool(re.search(r'^#define HAVE_DIAGNOSTIC 1$', configuration, re.M)),
        'configure WiredTiger with -DHAVE_DIAGNOSTIC=1 and rebuild')
print('Ensuring the engine and PALite match the current source', flush=True)
subprocess.run(['cmake', '--build', str(build), '--target', 'wiredtiger_shared',
                'wiredtiger_palite', '-j', '8'], cwd=root, check=True)
flags = ['-D_GNU_SOURCE', '-g', '-Og', '-Wall', '-Wextra', '-Werror']
if platform.machine() == 'aarch64':
    flags.append('-march=armv8.2-a+rcpc+crc')
if cache['CMAKE_BUILD_TYPE'] == 'ASan':
    require('clang' in pathlib.Path(compiler).name, 'ASan requires Clang and its shared runtime')
    flags += ['-fsanitize=address', '-shared-libasan', '-fno-omit-frame-pointer']
    runtime = subprocess.check_output([compiler, '--print-runtime-dir'], text=True).strip()
    flags.append(f'-Wl,-rpath,{runtime}')
command = [compiler, *flags, '-I', str(build / 'include'), '-I', str(build / 'config'),
           '-I', 'src/include', '-I', 'src/cursor', '-I', 'src/reconcile',
           'tools/stepdown_repro/repro.c', 'tools/stepdown_repro/trace.c', '-L', str(build),
           f'-Wl,-rpath,{build}', '-lwiredtiger', '-lpthread', '-o', args.output]
print(shlex.join(command), flush=True)
subprocess.run(command, cwd=root, check=True)
output = (root / args.output).resolve()
def checksum(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

source = root / 'src/reconcile/rec_write.c'
match = re.search(r'const u_int failpoint_probability = (\d+);', source.read_text())
require(match is not None, 'cannot identify pre-wrapup probability in source')
threshold = int(match[1])
library = build / 'libwiredtiger.so'
palite = build / 'ext/page_log/palite/libwiredtiger_palite.so'
manifest = dict(binary_sha256=checksum(output), library_sha256=checksum(library),
                palite_sha256=checksum(palite), rec_write_sha256=checksum(source),
                config_sha256=checksum(build / 'config/wiredtiger_config.h'),
                build=str(build), compiler=compiler,
                compiler_version=subprocess.check_output([compiler, '--version'], text=True),
                build_type=cache['CMAKE_BUILD_TYPE'], compile_command=command,
                failpoint_threshold=threshold, nominal_probability=(threshold + 1) / 10000)
output.with_name(output.name + '.build.json').write_text(json.dumps(manifest, indent=2) + '\n')
