import filecmp, os, glob, wiredtiger, wttest
from wtdataset import SimpleDataSet
from wtscenario import make_scenarios
from wtbackup import backup_base


# test_live_restore04.py
# Test using the wt utility with live restore.
@wttest.skip_for_hook("tiered", "using multiple WT homes")
class test_live_restore04(backup_base):
    format_values = [
        # ('column', dict(key_format='r', value_format='S')),
        ('row_integer', dict(key_format='i', value_format='S')),
    ]
    scenarios = make_scenarios(format_values)
    nrows = 10000
    ntables = 3
    conn_config = 'log=(enabled)'

    def get_stat(self, statistic):
        stat_cursor = self.session.open_cursor("statistics:")
        val = stat_cursor[statistic][2]
        stat_cursor.close()
        return val

    def test_live_restore04(self):
        # FIXME-WT-14051: Live restore is not supported on Windows.
        if os.name == 'nt':
            self.skipTest('Unix specific test skipped on Windows')

        # Create a folder to save the wt utility output.
        util_out_path = 'UTIL'
        os.mkdir(util_out_path)

        uris = []
        for i in range(self.ntables):
            uri = f'file:collection-{i}'
            uris.append(uri)
            ds = SimpleDataSet(self, uri, self.nrows, key_format=self.key_format,
                               value_format=self.value_format)
            ds.populate()

        self.session.checkpoint()

        # Dump file data for later comparison.
        # for i in range(self.ntables):
        #     dump_out = os.path.join(util_out_path, f'{uris[i]}.out')
        #     self.runWt(['dump', '-x', uris[i]], outfilename=dump_out)
        config = 'incremental=(enabled,granularity=1M,this_id="ID1")'
        bkup_c = self.session.open_cursor('backup:', None, config)

        # Now make a full backup.
        # Close the default connection.
        os.mkdir("SOURCE") 
        all_files = self.take_full_backup("SOURCE", bkup_c)
        self.close_conn()

        # Remove everything but SOURCE / stderr / stdout / util output folder.
        for f in glob.glob("*"):
            if not f == "SOURCE" and not f == "UTIL" and not f == "stderr.txt" and not f == "stdout.txt":
                os.remove(f)

        # Open a live restore connection with no background migration threads to leave it in an
        # unfinished state.
        self.open_conn(config="log=(enabled),statistics=(all),live_restore=(enabled=true,path=\"SOURCE\",threads_max=0)")
        # self.setupSessionOpen()
        os.mkdir("BACKUP")
        self.session.breakpoint()

        for i in uris:
            cur = self.session.open_cursor(i)
            for j in range(1, 1000):
                cur[ds.key(j)] = "abc"*100
        self.session.checkpoint()
        (bkup_files, _) = self.take_incr_backup("BACKUP", 1, 2)

        for i in bkup_files:
            self.pr(i)
