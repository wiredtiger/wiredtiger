# Read the source files and output the statistics #defines plus the
# initialize and clear code.

import re, string, sys, textwrap
from dist import compare_srcfile
from dist import source_paths_list

# Read the source files.
from stat_data import dsrc_stats, connection_stats

def print_struct(title, name, stats):
	'''Print the structures for the stat.h file.'''
	f.write('/*\n')
	f.write(' * Statistics entries for ' + title + '.\n')
	f.write(' */\n')
	f.write('struct __wt_' + name + '_stats {\n')

	for l in stats:
		f.write('\tWT_STATS ' + l.name + ';\n')
	f.write('};\n\n')

# Update the #defines in the stat.h file.
tmp_file = '__tmp'
f = open(tmp_file, 'w')
skip = 0
for line in open('../src/include/stat.h', 'r'):
	if not skip:
		f.write(line)
	if line.count('Statistics section: END'):
		f.write(line)
		skip = 0
	elif line.count('Statistics section: BEGIN'):
		f.write('\n')
		skip = 1
		print_struct('data sources', 'dsrc', dsrc_stats)
		print_struct('connections', 'connection', connection_stats)
f.close()
compare_srcfile(tmp_file, '../src/include/stat.h')

def print_defines():
	'''Print the #defines for the wiredtiger.in file.'''
	f.write('''
/*!
 * @name Connection statistics
 * @anchor statistics_keys
 * @anchor statistics_conn
 * Statistics are accessed through cursors with \c "statistics:" URIs.
 * Individual statistics can be queried through the cursor using the following
 * keys.  See @ref data_statistics for more information.
 * @{
 */
''')
	for v, l in enumerate(connection_stats):
		f.write('/*! %s */\n' % '\n * '.join(textwrap.wrap(l.desc, 70)))
		f.write('#define\tWT_STAT_CONN_' + l.name.upper() + "\t" *
		    max(1, 6 - int((len('WT_STAT_CONN_' + l.name)) / 8)) +
		    str(v) + '\n')
	f.write('''
/*!
 * @}
 * @name Statistics for data sources
 * @anchor statistics_dsrc
 * @{
 */
''')
	for v, l in enumerate(dsrc_stats):
		f.write('/*! %s */\n' % '\n * '.join(textwrap.wrap(l.desc, 70)))
		f.write('#define\tWT_STAT_DSRC_' + l.name.upper() + "\t" *
		    max(1, 6 - int((len('WT_STAT_DSRC_' + l.name)) / 8)) +
		    str(v) + '\n')
	f.write('/*! @} */\n')

# Update the #defines in the wiredtiger.in file.
tmp_file = '__tmp'
f = open(tmp_file, 'w')
skip = 0
for line in open('../src/include/wiredtiger.in', 'r'):
	if not skip:
		f.write(line)
	if line.count('Statistics section: END'):
		f.write(line)
		skip = 0
	elif line.count('Statistics section: BEGIN'):
		f.write(' */\n')
		skip = 1
		print_defines()
		f.write('/*\n')
f.close()
compare_srcfile(tmp_file, '../src/include/wiredtiger.in')

def print_func(name, list):
	'''Print the functions for the stat.c file.'''
	f.write('''
void
__wt_stat_init_''' + name + '''_stats(WT_''' + name.upper() + '''_STATS *stats)
{
''')
	for l in sorted(list):
		o = '\tstats->' + l.name + '.desc = "' + l.desc + '";\n'
		if len(o) + 7  > 80:
			o = o.replace('= ', '=\n\t    ')
		f.write(o)
	f.write('''}
''')

	f.write('''
void
__wt_stat_clear_''' + name + '''_stats(void *stats_arg)
{
\tWT_''' + name.upper() + '''_STATS *stats;

\tstats = (WT_''' + name.upper() + '''_STATS *)stats_arg;
''')
	for l in sorted(list):
		# no_clear: don't clear the value.
		if not 'no_clear' in l.flags:
			f.write('\tstats->' + l.name + '.v = 0;\n');
	f.write('}\n')

	# Aggregation is only interesting for data-source statistics.
	if name == 'connection':
		return;

	f.write('''
void
__wt_stat_aggregate_''' + name + '''_stats(void *child, void *parent)
{
\tWT_''' + name.upper() + '''_STATS *c, *p;

\tc = (WT_''' + name.upper() + '''_STATS *)child;
\tp = (WT_''' + name.upper() + '''_STATS *)parent;
''')
	for l in sorted(list):
		if 'no_aggregate' in l.flags:
			continue;
		elif 'max_aggregate' in l.flags:
			o = 'if (c->' + l.name + '.v > p->' + l.name +\
			'.v)\n\t    p->' + l.name + '.v = c->' + l.name + '.v;'
		else:
			o = 'p->' + l.name + '.v += c->' + l.name + '.v;'
		f.write('\t' + o + '\n')
	f.write('}\n')

# Write the stat initialization and clear routines to the stat.c file.
f = open(tmp_file, 'w')
f.write('/* DO NOT EDIT: automatically built by dist/stat.py. */\n\n')
f.write('#include "wt_internal.h"\n')

print_func('dsrc', dsrc_stats)
print_func('connection', connection_stats)
f.close()
compare_srcfile(tmp_file, '../src/support/stat.c')


# Update the statlog file with the entries we can scale per second.
scale_info = 'no_scale_per_second_list = [\n'
for l in sorted(connection_stats):
	if 'no_scale' in l.flags:
		scale_info += '    \'' + l.desc + '\',\n'
for l in sorted(dsrc_stats):
	if 'no_scale' in l.flags:
		scale_info += '    \'' + l.desc + '\',\n'
scale_info += ']\n'

tmp_file = '__tmp'
tfile = open(tmp_file, 'w')
skip = 0
for line in open('../tools/statlog.py', 'r'):
	if skip:
		if line.count('no scale-per-second list section: END'):
			tfile.write(line)
			skip = 0
	else:
		tfile.write(line)
	if line.count('no scale-per-second list section: BEGIN'):
		skip = 1
		tfile.write(scale_info)
tfile.close()
compare_srcfile(tmp_file, '../tools/statlog.py')
