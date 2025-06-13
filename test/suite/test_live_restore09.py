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
    conn_config = 'log=(enabled=true)'

    def get_stat(self, statistic):
        stat_cursor = self.session.open_cursor("statistics:")
        val = stat_cursor[statistic][2]
        stat_cursor.close()
        return val

    @unittest.skip("Skipping")
    def test_live_restore_simple_incremental_scenario(self):
        # FIXME-WT-14051: Live restore is not supported on Windows.
        if os.name == 'nt':
            self.skipTest('Unix specific test skipped on Windows')

        uris = []
        for i in range(self.ntables):
            uri = f'file:collection-{i}'
            uris.append(uri)
            ds = SimpleDataSet(self, uri, self.nrows, key_format='i')
            ds.populate()

        self.session.checkpoint()
        config = 'incremental=(enabled,granularity='+self.granularity + ',this_id="ID1")'
        bkup_c = self.session.open_cursor('backup:', None, config)

        # Now take a full backup into the SOURCE directory.
        os.mkdir("SOURCE")
        all_files = self.take_full_backup("SOURCE", bkup_c)

        # Close the connection.
        self.close_conn()
        # Remove everything but SOURCE / stderr / stdout / util output folder.
        for f in glob.glob("*"):
            if not f == "SOURCE" and not f == "UTIL" and not f == "stderr.txt" and not f == "stdout.txt":
                os.remove(f)

        # Open a live restore connection with no background migration threads to leave it in an
        # unfinished state.
        self.open_conn(config="statistics=(all),live_restore=(enabled=true,path=\"SOURCE\",threads_max=0,read_size="+self.granularity + ")")
        # Verbose config.
        #self.open_conn(config="verbose=(live_restore:2),statistics=(all),live_restore=(enabled=true,path=\"SOURCE\",threads_max=0,read_size="+self.granularity + ")")
        self.conn.set_timestamp('oldest_timestamp=1,stable_timestamp=1')

        key1 = "abc"*100
        key2 = "def"*100
        # Do a series of smaller modifications.
        for i in uris:
            cur = self.session.open_cursor(i)
            for j in range(1, 1000):
                self.session.begin_transaction()
                cur[ds.key(j)] = key1
                self.session.commit_transaction('commit_timestamp=2')
                self.session.begin_transaction()
                cur[ds.key(j)] = key2
                self.session.commit_transaction('commit_timestamp=3')

        self.conn.set_timestamp('stable_timestamp=4')
        # Take a checkpoint.
        self.session.checkpoint()
        # Do an incremental backup of the current state of the database.
        (bkup_files, _) = self.take_live_restore_incr_backup("BACKUP", "SOURCE", 1, 2)

        # Debug code.
        for i in bkup_files:
            self.pr(i)
        # Close the connection, open it on the now backed up live restore.
        self.close_conn()
        self.open_conn(directory='BACKUP', config="log=(enabled),statistics=(all)")

        # Validate that the new data is there.
        for i in uris:
            cur = self.session.open_cursor(i)
            for j in range(1, 1000):
                val = cur[ds.key(j)]
                assert(val == key2)
                cur.reset()
                self.session.begin_transaction('read_timestamp=2')
                val = cur[ds.key(j)]
                self.pr(val)
                assert(val == key1)
                self.session.rollback_transaction()

        self.close_conn()
        # Open the original live restore and continue. This time with background threads.
        self.open_conn(config="statistics=(all),live_restore=(enabled=true,path=\"SOURCE\",threads_max=4,read_size="+self.granularity + ")")
        # Loop until completion.
        # TODO: This ^.

    @unittest.skip("this once again isn't working.")
    def test_live_restore_simple_incremental_scenario(self):
        # FIXME-WT-14051: Live restore is not supported on Windows.
        if os.name == 'nt':
            self.skipTest('Unix specific test skipped on Windows')

        # The goal of this test is to generate a large amount of unstable content, so that recovery
        # will roll back entire pages. The recovery checkpoint will then write out these changes, we
        # want to know that our incremental backup cursor will catch those recovery checkpoint
        # changes.
        uris = []
        for i in range(self.ntables):
            uri = f'file:collection-{i}'
            uris.append(uri)
            ds = SimpleDataSet(self, uri, self.nrows, key_format='i', config='log=(enabled=false)')
            ds.populate()

        self.conn.set_timestamp('oldest_timestamp=1,stable_timestamp=1')
        self.session.checkpoint()

        # Now take a full backup into the SOURCE directory.'
        bkup_c = self.session.open_cursor('backup:', None, 'incremental=(enabled,granularity='+self.granularity + ',this_id="ID1")')
        os.mkdir("SOURCE")
        all_files = self.take_full_backup("SOURCE", bkup_c)
        bkup_c.close()

        # Do a series of unstable modifications.
        key1 = "abc"*100
        for i in uris:
            cur = self.session.open_cursor(i)
            for j in range(1, self.nrows):
                self.session.begin_transaction()
                cur[ds.key(j)] = key1
                self.session.commit_transaction('commit_timestamp=2')

        # All of this content should be in data store, and the rest in the history store.
        self.session.checkpoint()

        # Take an incremental backup.
        (bkup_files, _) = self.take_incr_backup("SOURCE", 1, 2)

        # Close the connection.
        self.pr("0")
        self.close_conn()

        # Remove everything but SOURCE / stderr / stdout / util output folder.
        for f in glob.glob("*"):
            if not f == "SOURCE" and not f == "UTIL" and not f == "stderr.txt" and not f == "stdout.txt":
                os.remove(f)

        # Open a live restore connection with no background migration threads to leave it in an
        # unfinished state.
        self.pr("1")
        self.open_conn(config="verbose=(live_restore:3,recovery_progress:1,read:3,write:3,backup:3),statistics=(all),live_restore=(enabled=true,path=\"SOURCE\",threads_max=0,read_size="+self.granularity + ")")

        # Check that the unstable content was RTS'd.
        for i in uris:
            cur = self.session.open_cursor(i)
            assert(cur[ds.key(1)] != key1)
        # Do an incremental backup of the current state of the database.
        # self.session.breakpoint()
        (bkup_files, _) = self.take_live_restore_incr_backup("BACKUP", "SOURCE", 1, 2)
        # exit()
        # Close the connection, open it on the now backed up live restore.
        self.pr("2")
        self.close_conn()
        self.pr("3")
        self.open_conn(directory='BACKUP', config="verbose=(live_restore:3,recovery_progress:1,read:3,write:3,backup:3),log=(enabled),statistics=(all)")
        self.pr("4")
        # Validate that the old data is there.
        self.session.breakpoint()
        for i in uris:
            cur = self.session.open_cursor(i)
            for j in range(nrows):
                val = cur[ds.key(j)]
                assert(val == ds.value(j))

    #@unittest.skip("another day another scenario")
    def test_no_live_restore(self):
        # FIXME-WT-14051: Live restore is not supported on Windows.
        if os.name == 'nt':
            self.skipTest('Unix specific test skipped on Windows')

        # The goal of this test is to generate a large amount of unstable content, so that recovery
        # will roll back entire pages. The recovery checkpoint will then write out these changes, we
        # want to know that our incremental backup cursor will catch those recovery checkpoint
        # changes.
        uris = []
        for i in range(self.ntables):
            uri = f'file:collection-{i}'
            uris.append(uri)
            ds = SimpleDataSet(self, uri, self.nrows, key_format='i', config='log=(enabled=false)')
            ds.populate()

        self.conn.set_timestamp('oldest_timestamp=1,stable_timestamp=1')
        self.session.checkpoint()

        # Now take a full backup into the SOURCE directory.'
        bkup_c = self.session.open_cursor('backup:', None, 'incremental=(enabled,granularity='+self.granularity + ',this_id="ID1")')
        os.mkdir("SOURCE")
        all_files = self.take_full_backup("SOURCE", bkup_c)
        bkup_c.close()

        # Do a series of unstable modifications.
        key1 = "abc"*100
        for i in uris:
            cur = self.session.open_cursor(i)
            for j in range(1, self.nrows):
                self.session.begin_transaction()
                cur[ds.key(j)] = key1
                self.session.commit_transaction('commit_timestamp=2')

        # All of this content should be in data store, and the rest in the history store.
        self.session.checkpoint()

        # Take an incremental backup.
        (bkup_files, _) = self.take_incr_backup("SOURCE", 1, 2)
        shutil.copytree("SOURCE", "SOURCE_TMP")

        # Close the connection.
        self.pr("0")
        self.close_conn()

        # Open a live restore connection with no background migration threads to leave it in an
        # unfinished state.
        self.pr("1")
        self.open_conn(directory='SOURCE_TMP', config="verbose=(live_restore:3,recovery_progress:1,read:3,write:3,backup:3),statistics=(all)")

        # Check that the unstable content was RTS'd.
        for i in uris:
            cur = self.session.open_cursor(i)
            assert(cur[ds.key(1)] != key1)
        # Do an incremental backup of the current state of the database.
        # self.session.breakpoint()
        (bkup_files, _) = self.take_incr_backup("SOURCE", 1, 2)
        # exit()
        # Close the connection, open it on the now backed up live restore.
        self.pr("2")
        self.close_conn()
        self.pr("3")
        self.open_conn(directory='SOURCE', config="verbose=(live_restore:3,recovery_progress:1,read:3,write:3,backup:3),log=(enabled),statistics=(all)")
        self.pr("4")
        # Validate that the old data is there.
        self.session.breakpoint()
        for i in uris:
            cur = self.session.open_cursor(i)
            for j in range(nrows):
                val = cur[ds.key(j)]
                assert(val == ds.value(j))

    @unittest.skip("This was useful")
    def test_live_restore_single_uri_key_incremental_scenario(self):
        # FIXME-WT-14051: Live restore is not supported on Windows.
        if os.name == 'nt':
            self.skipTest('Unix specific test skipped on Windows')

        #self.conn.set_timestamp('oldest_timestamp=1,stable_timestamp=1')


        # Do a series of unstable modifications.
        uri = "file:abc.wt"
        value1 = "a" * 100
        value2 = "b" * 100
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
        cur[1] = value2
        self.session.checkpoint()
        (bkup_files, _) = self.take_incr_backup("SOURCE", 1, 2)
        # Close the connection.
        self.pr("0")
        self.close_conn()
        # Remove everything but SOURCE / stderr / stdout / util output folder.
        for f in glob.glob("*"):
            if not f == "SOURCE" and not f == "UTIL" and not f == "stderr.txt" and not f == "stdout.txt":
                os.remove(f)

        # Open a live restore connection with no background migration threads to leave it in an
        # unfinished state.
        self.pr("1")
        self.open_conn(config="verbose=(live_restore:3,recovery_progress:1,read:3,write:3,backup:3),statistics=(all),live_restore=(enabled=true,path=\"SOURCE\",threads_max=0,read_size="+self.granularity + ")")
        # Do an incremental backup of the current state of the database.
        (bkup_files, _) = self.take_live_restore_incr_backup("BACKUP", "SOURCE", 1, 2)
        exit()
        # Close the connection, open it on the now backed up live restore.
        self.pr("2")
        self.close_conn()
        self.pr("3")
        self.open_conn(directory='BACKUP', config="verbose=(block:1,recovery:1,recovery_progress:1,read:3,write:3),log=(enabled),statistics=(all)")
        selp.pr("4")
        # Validate that the old data is there.
        self.session.breakpoint()
        for i in uris:
            cur = self.session.open_cursor(i)
            for j in range(nrows):
                val = cur[ds.key(j)]
                assert(val == ds.value(j))