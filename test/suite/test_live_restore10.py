import filecmp, os, glob, wiredtiger, wttest, unittest,shutil
from wtdataset import SimpleDataSet
from wtscenario import make_scenarios
from wtbackup import backup_base


# test_live_restore04.py
# Test backup during a live restore, this test has a number of steps:
# 1: Create a regular DB with a SimpleDataSet
# 2: Take a full backup with incremental ID1.
# 3: Using that full backup, copy it to the SOURCE/ path
# 4: Begin a live restore from SOURCE/ specifying 0 background threads to avoid it completing the
# restore.
# 5: Take a backup of that system. If the backup granlurity matches the WiredTiger block size the
@wttest.skip_for_hook("tiered", "using multiple WT homes")
class test_live_restore04(backup_base):
    granularities = [
        ('1M', dict(granularity='1M')),
        #('4K', dict(granularity='4K')),
        #('512K', dict(granularity='512K'))
    ]
    scenarios = make_scenarios(granularities)
    nrows = 10000
    ntables = 3
    #conn_config = 'log=(enabled=true)'

    def get_stat(self, statistic):
        stat_cursor = self.session.open_cursor("statistics:")
        val = stat_cursor[statistic][2]
        stat_cursor.close()
        return val


    def test_live_restore_single_uri_key_incremental_scenario(self):
        # FIXME-WT-14051: Live restore is not supported on Windows.
        if os.name == 'nt':
            self.skipTest('Unix specific test skipped on Windows')

        # Do a series of unstable modifications.
        uri = "file:abc.wt"
        value1 = "a" * 100
        value2 = "b" * 100
        value3 = "c" * 100
        value4 = "ddd" * 100
        value5 = "eee" * 100
        value6 = "fff" * 100

        # Test options:
        live_restore=True
        include_unstable=False
        make_stable=False
        
        self.session.create(uri, "key_format=i,value_format=S")
        cur = self.session.open_cursor(uri)
        cur[1] = value1
        self.session.checkpoint()

        config = 'incremental=(enabled,granularity='+self.granularity + ',this_id="ID1")'
        bkup_c = self.session.open_cursor('backup:', None, config)

        # Take a full backup into the SOURCE directory.
        os.mkdir("SOURCE")
        all_files = self.take_full_backup("SOURCE", bkup_c)
        bkup_c.close()
        self.conn.set_timestamp('oldest_timestamp=1,stable_timestamp=1')
        cur[1] = value2

        if (include_unstable):
            for i in range(2, 10000):
                self.session.begin_transaction()
                cur[i] = value5
                self.session.commit_transaction('commit_timestamp=2')
                self.session.begin_transaction()
                cur[i] = value6
                self.session.commit_transaction('commit_timestamp=3')

        if (make_stable):
            self.conn.set_timestamp('stable_timestamp=4')

        self.session.checkpoint()
        (bkup_files, _) = self.take_incr_backup("SOURCE", 1, 2)

        if (not live_restore):
            shutil.copytree("SOURCE", "SOURCE_TMP")

        # Close the connection.
        self.pr("0")
        self.close_conn()
        # Remove everything but SOURCE / stderr / stdout / util output folder.

        if (live_restore):
            for f in glob.glob("*"):
                if not f == "SOURCE" and not f == "UTIL" and not f == "stderr.txt" and not f == "stdout.txt":
                    os.remove(f)

        # Open a live restore connection with no background migration threads to leave it in an
        # unfinished state.
        self.pr("1")
        config = "verbose=(live_restore:3,recovery_progress:1,read:3,write:3,backup:3),statistics=(all)"
        if (live_restore):
            config += ",live_restore=(enabled=true,path=\"SOURCE\",threads_max=0,read_size="+self.granularity + ")"
            self.open_conn(config=config)
        else:
            self.open_conn("SOURCE_TMP", config)

        cur = self.session.open_cursor(uri)

        cur[1] = value3
        for i in range (2, 100000):
            self.session.begin_transaction()
            cur[i] = value4
            self.session.commit_transaction('commit_timestamp=10')
        self.session.breakpoint()
        self.session.checkpoint()
        
        # Do an incremental backup of the current state of the database.
        if (live_restore):
            self.pr("Live restore incremental backup")
            (bkup_files, _) = self.take_live_restore_incr_backup("BACKUP", "SOURCE", self.granularity, 1, 2)
        else:
            os.mkdir("BACKUP")
            self.session.breakpoint()
            (bkup_files) = self.take_full_backup("BACKUP", None, "SOURCE_TMP")


        # # Close the connection, open it on the now backed up data.
        # self.close_conn()
        # self.pr("3")
        # self.open_conn(directory='BACKUP', config="verbose=(block:1,recovery:1,recovery_progress:1,read:3,write:3),statistics=(all)")
        # self.pr("4")
        # # Validate that the old data is there.
        # self.session.breakpoint()
        # cur = self.session.open_cursor(uri)
        # assert(cur[1] == value3)
        # for i in range (2, 10000):
        #     assert(cur[i] ==  value4)
        # self.session.checkpoint()