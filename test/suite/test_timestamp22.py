#!/usr/bin/env python
#
# Public Domain 2014-2020 MongoDB, Inc.
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
# test_timestamp22.py
# Misuse the timestamp API, making sure we don't crash.
import wiredtiger, wttest, re, suite_random
from wtdataset import SimpleDataSet
from contextlib import contextmanager

def timestamp_str(t):
    return '%x' % t

class test_timestamp22(wttest.WiredTigerTestCase):
    conn_config = 'cache_size=50MB,statistics=(all)'
    session_config = 'isolation=snapshot'
    iterations = 1000
    nrows = 10
    uri = "table:test_timestamp22"
    rand = suite_random.suite_random()
    oldest_ts = 0
    stable_ts = 0
    SUCCESS = 'success'
    FAILURE = 'failure'

    # Control execution of an operation, looking for exceptions and error messages.
    # Usage:
    #  with self.expect(self.FAILURE, 'some operation'):
    #     some_operation()  # In this case, we expect it will fail
    @contextmanager
    def expect(self, expected, message):
        self.pr('TRYING: ' + message + ', expect ' + expected)
        got = None
        # If there are stray error messages from a previous operation,
        # let's find out now.  It can be confusing if we do something illegal
        # here and we have multiple messages to sort out.
        self.checkStderr()

        # 'yield' runs the subordinate operation, we'll catch any resulting exceptions.
        try:
            if expected == self.FAILURE:
                # Soak up any error messages that happen as a result of the failure.
                with self.expectedStderrPattern(r'^.*$', re_flags=re.MULTILINE):
                    yield
            else:
                yield
            got = self.SUCCESS
        except:
            got = self.FAILURE
            self.cleanStderr()

        message += ' got ' + got

        # If we're about to assert, show some extra info
        if expected != got:
            message += ': ERROR expected ' + expected
            self.checkStderr()
        self.pr(message)
        self.assertEquals(expected, got)

    # Create a predictable value based on the iteration number and timestamp.
    def gen_value(self, iter, ts):
        return str(iter) + '_' + str(ts) + '_' + 'x' * 1000

    # Given a number representing an "approximate timestamp", generate a timestamp
    # that is near that number, either plus or minus.
    def gen_ts(self, approx_ts):
        # a number between -10 and 10:
        n = self.rand.rand32() % 21 - 10
        ts = approx_ts + n
        if ts <= 0:
            ts = 1
        return ts

    # Asks whether we should do an illegal operation now. Return yes 5%.
    def do_illegal(self):
        return self.rand.rand32() % 20 == 0

    def report(self, func, arg = None):
        self.pr('DOING: ' + func + ('' if arg == None else '(' + arg + ')'))

    # Insert a set of rows, each insert in its own transaction, with the
    # given timestamps.
    def updates(self, value, ds, do_prepare, commit_ts, durable_ts):
        session = self.session
        needs_rollback = False
        prepare_config = None
        commit_config = 'commit_timestamp=' + timestamp_str(commit_ts)

        bad_commit = not do_prepare and self.do_illegal()
        bad_prepare = False
        # Occasionally put a durable timestamp on a commit without a prepare,
        # that will be an error.
        if do_prepare or bad_commit:
            commit_config += ',durable_timestamp=' + timestamp_str(durable_ts)
        cursor = session.open_cursor(self.uri)
        prepare_ts = self.gen_ts(commit_ts)
        prepare_config = 'prepare_timestamp=' + timestamp_str(prepare_ts)

        # Predict whether the commit and optional prepare will fail.
        if commit_ts < self.oldest_ts or commit_ts < self.stable_ts:
            bad_commit = True
        if do_prepare:
            if commit_ts < prepare_ts:
                bad_commit = True
            if prepare_ts < self.oldest_ts:
                bad_prepare = True
            # If the prepare fails, the commit will as well.
            if bad_prepare:
                bad_commit = True
        msg = 'inserts with commit config(' + commit_config + ')'

        try:
            for i in range(0, self.nrows):
                needs_rollback = False
                if self.do_illegal():
                    # Illegal outside of transaction
                    self.report('prepare_transaction', prepare_config)
                    with self.expect(self.FAILURE, 'prepare outside of transaction'):
                        session.prepare_transaction(prepare_config)

                with self.expect(self.SUCCESS, 'begin_transaction'):
                    session.begin_transaction()
                    needs_rollback = True

                self.report('set key/value')
                with self.expect(self.SUCCESS, 'cursor insert'):
                    cursor[ds.key(i)] = value

                if do_prepare:
                    self.report('prepare_transaction', prepare_config)
                    with self.expect(self.FAILURE if bad_prepare else self.SUCCESS, 'prepare'):
                        session.prepare_transaction(prepare_config)

                # If we did a successful prepare and are set up (by virtue of bad timestamps)
                # to do a bad commit, WT will panic, and the test cannot continue.
                # Only proceed with the commit if we have don't have that particular case.
                if not bad_commit or not do_prepare or bad_prepare:
                    needs_rollback = False
                    self.report('commit_transaction', commit_config)
                    with self.expect(self.FAILURE if bad_commit else self.SUCCESS, 'commit'):
                        session.commit_transaction(commit_config)
                        self.commit_value = value
                if needs_rollback:
                    self.report('rollback_transaction')
                    needs_rollback = False
                    session.rollback_transaction()
        except Exception as e:
            # We don't expect any exceptions, they should be caught as part of self.expect statements.
            self.pr(msg + 'UNEXPECTED EXCEPTION!')
            self.pr(msg + 'fail: ' + str(e))
            raise e
        if needs_rollback:
            self.report('rollback_transaction')
            session.rollback_transaction()
        cursor.close()

    def make_timestamp_config(self, oldest, stable):
        config = ''
        if oldest >= 0:
            config = 'oldest_timestamp=' + timestamp_str(oldest)
        if stable >= 0:
            if config != '':
                config += ','
            config += 'stable_timestamp=' + timestamp_str(stable)
        return config

    # Determine whether we expect the set_timestamp to succeed.
    def expected_result_set_timestamp(self, oldest, stable):
        expected = self.SUCCESS
        if oldest >= 0:
            if stable >= 0:
                if oldest > stable:
                    expected = self.FAILURE
            elif oldest > self.stable_ts:
                expected = self.FAILURE
        elif stable >= 0 and self.oldest_ts > stable:
            expected = self.FAILURE
        return expected

    def set_global_timestamps(self, oldest, stable):
        config = self.make_timestamp_config(oldest, stable)
        expected = self.expected_result_set_timestamp(oldest, stable)

        with self.expect(expected, 'set_timestamp(' + config + ')'):
            self.conn.set_timestamp(config)

        # Predict what we expect to happen to the timestamps.
        if expected == self.SUCCESS:
            # If that passes, then independently, oldest and stable can advance, but if they
            # are less than the current value, that is silently ignored.
            if oldest >= self.oldest_ts:
                self.oldest_ts = oldest
                self.pr('updating oldest: ' + str(oldest))
            if stable >= self.stable_ts:
                self.stable_ts = stable
                self.pr('updating stable: ' + str(stable))

        # Make sure the state of global timestamps is what we think.
        expect_query_oldest = timestamp_str(self.oldest_ts)
        expect_query_stable = timestamp_str(self.stable_ts)
        query_oldest = self.conn.query_timestamp('get=oldest')
        query_stable = self.conn.query_timestamp('get=stable')

        self.assertEquals(expect_query_oldest, query_oldest)
        self.assertEquals(expect_query_stable, query_stable)
        self.pr('oldest now: ' + query_oldest)
        self.pr('stable now: ' + query_stable)

        if expected == self.FAILURE:
            self.cleanStderr()

    def test_timestamp(self):
        create_params = 'value_format=S,key_format=i'
        self.session.create(self.uri, create_params)

        self.set_global_timestamps(1, 1)

        # Create tables with no entries
        ds = SimpleDataSet(
            self, self.uri, 0, key_format="i", value_format="S", config='log=(enabled=false)')

        # We do a bunch of iterations, doing transactions, prepare, and global timestamp calls
        # with timestamps that are sometimes valid, sometimes not. We use the iteration number
        # as a "approximate timestamp", and generate timestamps for our calls that are near
        # that number (within 10).  Thus, as the test runs, the timestamps generally get larger.
        # We always know the state of global timestamps, so we can predict the success/failure
        # or each call.
        self.commit_value = '<NOT_SET>'
        for iter in range(1, self.iterations):
            self.pr('\n===== ITERATION ' + str(iter) + '/' + str(self.iterations))
            self.pr('RANDOM: ({0},{1})'.format(self.rand.seedw,self.rand.seedz))
            if self.rand.rand32() % 10 != 0:
                commit_ts = self.gen_ts(iter)
                durable_ts = self.gen_ts(iter)
                do_prepare = (self.rand.rand32() % 20 == 0)
                if do_prepare:
                    # If we doing a prepare, we must abide by some additional rules.
                    # If we don't we'll immediately panic
                    if commit_ts < self.oldest_ts:
                        commit_ts = self.oldest_ts
                    if durable_ts < commit_ts:
                        durable_ts = commit_ts
                value = self.gen_value(iter, commit_ts)
                self.updates(value, ds, do_prepare, commit_ts, durable_ts)

            r = self.rand.rand32() % 5
            if r == 0:
                # Set both global timestamps
                self.set_global_timestamps(self.gen_ts(iter), self.gen_ts(iter))
            elif r == 1:
                # Set oldest timestamp
                self.set_global_timestamps(self.gen_ts(iter), -1)
            elif r == 2:
                # Set stable timestamp
                self.set_global_timestamps(-1, self.gen_ts(iter))

        # Make sure the resulting rows are what we expect.
        cursor = self.session.open_cursor(self.uri)
        expect_key = 0
        expect_value = self.commit_value
        for k,v in cursor:
            self.assertEquals(k, expect_key)
            self.assertEquals(v, expect_value)
            expect_key += 1

        # Although it's theoretically possible to never successfully update a single row,
        # with a large number of iterations that should never happen.  I'd rather catch
        # a test code error where we mistakenly don't update any rows.
        self.assertGreater(expect_key, 0)
        cursor.close()

if __name__ == '__main__':
    wttest.run()
