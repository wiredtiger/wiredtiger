#!/usr/bin/env python

import os, re, sys, textwrap
import api_data
from dist import compare_srcfile

# Temporary file.
tmp_file = '__tmp'

#####################################################################
# Update wiredtiger.in with doxygen comments
#####################################################################
f='../src/include/wiredtiger.in'
tfile = open(tmp_file, 'w')

cbegin_re = re.compile(r'(\s*\*\s*)@config(?:empty|start)\{(.*?),.*\}')

def gettype(c):
	'''Derive the type of a config item'''
	checks = c.flags
	ctype = checks.get('type', None)
	if not ctype and ('min' in checks or 'max' in checks):
		ctype = 'int'
	return ctype or 'string'

def typedesc(c):
	'''Descripe what type of value is expected for the given config item'''
	checks = c.flags
	cmin = str(checks.get('min', ''))
	cmax = str(checks.get('max', ''))
	choices = checks.get('choices', [])
	ctype = gettype(c)
	desc = {
		'boolean' : 'a boolean flag',
		'format'  : 'a format string',
		'int'     : 'an integer',
		'list'    : 'a list',
		'string'  : 'a string'}[ctype]
	if cmin and cmax:
		desc += ' between ' + cmin + ' and ' + cmax
	elif cmin:
		desc += ' greater than or equal to ' + cmin
	elif cmax:
		desc += ' no more than ' + cmax
	if choices:
		if ctype == 'list':
			desc += ', with values chosen from the following options: '
		else:
			desc += ', chosen from the following options: '
		desc += ', '.join('\\c "' + c + '"' for c in choices)
	elif ctype == 'list':
		desc += ' of strings'
	return desc

skip = False
for line in open(f, 'r'):
	if skip:
		if '@configend' in line:
			skip = False
		continue

	m = cbegin_re.match(line)
	if not m:
		tfile.write(line)
		continue

	prefix, config_name = m.groups()
	if config_name not in api_data.methods:
		print >>sys.stderr, "Missing configuration for " + config_name
		tfile.write(line)
		continue

	skip = ('@configstart' in line)

	if not api_data.methods[config_name].config:
		tfile.write(prefix + '@configempty{' + config_name +
				', see dist/api_data.py}\n')
		continue

	tfile.write(prefix + '@configstart{' + config_name +
			', see dist/api_data.py}\n')

	w = textwrap.TextWrapper(width=80-len(prefix.expandtabs()),
			break_on_hyphens=False)
	lastname = None
	for c in sorted(api_data.methods[config_name].config):
		name = c.name
		if '.' in name:
			print >>sys.stderr, "Bad config key " + name

		# Deal with duplicates: with complex configurations (like
		# WT_SESSION::create), it's simpler to deal with duplicates here than
		# manually in api_data.py.
		if name == lastname:
			continue
		lastname = name
		if 'undoc' in c.flags:
			continue

		desc = textwrap.dedent(c.desc) + '.'
		desc = desc.replace(',', '\\,')
		default = '\\c ' + str(c.default) if c.default or gettype(c) == 'int' \
				else 'empty'
		tdesc = typedesc(c) + '; default ' + default + '.'
		tdesc = tdesc.replace(',', '\\,')
		output = '@config{' + ','.join((name, desc, tdesc)) + '}'
		for l in w.wrap(output):
			tfile.write(prefix + l + '\n')

	tfile.write(prefix + '@configend\n')

tfile.close()
compare_srcfile(tmp_file, f)

#####################################################################
# Create config_def.c with defaults for each config string
#####################################################################
f='../src/config/config_def.c'
tfile = open(tmp_file, 'w')

tfile.write('''/* DO NOT EDIT: automatically built by dist/config.py. */

#include "wt_internal.h"
''')

# Make a TextWrapper that can wrap at commas.
w = textwrap.TextWrapper(width=68, break_on_hyphens=False)
w.wordsep_re = w.wordsep_simple_re = re.compile(r'(,)')

def checkstr(c):
	'''Generate the JSON string used by __wt_config_check to validate the
	config string'''
	checks = c.flags
	cmin = str(checks.get('min', ''))
	cmax = str(checks.get('max', ''))
	choices = checks.get('choices', [])
	result = []
	if cmin:
		result.append('min=' + cmin)
	if cmax:
		result.append('max=' + cmax)
	if choices:
		result.append('choices=' + '[' +
		    ','.join('\\"' + s + '\\"' for s in choices) + ']')
	if result:
		return '"' + ','.join(result) + '"'
	else:
		return 'NULL'

def get_default(c):
	t = gettype(c)
	if c.default == 'false':
		return '0'
	elif (c.default or t == 'int') and c.default != 'true':
		return str(c.default)
	else:
		return ''

for name in sorted(api_data.methods.keys()):
	ctype = api_data.methods[name].config
	name = name.replace('.', '_')
	tfile.write('''
const char *
__wt_confdfl_%(name)s =
%(config)s;
''' % {
	'name' : name,
	'config' : '\n'.join('\t"%s"' % line
		for line in w.wrap(','.join('%s=%s' % (c.name, get_default(c))
			for c in sorted(ctype))) or [""]),
})
# Construct an array of allowable configuration options. Always append an empty
# string as a terminator for iteration
	if not ctype:
		tfile.write('''
WT_CONFIG_CHECK
__wt_confchk_%(name)s[] = {
\t{ NULL, NULL, NULL }
};
''' % { 'name' : name })
	else:
		tfile.write('''
WT_CONFIG_CHECK
__wt_confchk_%(name)s[] = {
\t%(check)s
\t{ NULL, NULL, NULL }
};
''' % {
	'name' : name,
	'check' : '\n\t'.join('"\n\t    "'.join(w.wrap('{ "%s", "%s", %s },' %
		(c.name, gettype(c), checkstr(c)))) for c in sorted(ctype)),
})

tfile.close()
compare_srcfile(tmp_file, f)
