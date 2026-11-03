#!/usr/bin/env python3
# Copyright (c) 2026 MongoDB, Inc.
# See the file LICENSE for redistribution information.

import unittest
from unittest.mock import patch

import atomic_review

class TestAtomicReview(unittest.TestCase):
    def test_detection(self):
        for name in atomic_review.HEADERS:
            self.assertTrue(atomic_review.needs_review([{'filename': name}]))
            self.assertTrue(atomic_review.needs_review([
                {'filename': 'renamed.h', 'previous_filename': name}]))
        for text in ['+__wt_atomic_load_bool_relaxed(p);', '-WT_FULL_BARRIER();',
            '+__wt_tsan_suppress_load_uint64(p);', ' __wt_atomic_store_uint64_release(\n+q,']:
            self.assertTrue(atomic_review.needs_review([{'filename': 'txn.c', 'patch': text}]))
        self.assertFalse(atomic_review.needs_review([{'filename': 'txn.c', 'patch': '+ordinary'}]))

    @patch('atomic_review.api')
    def test_requests(self, api):
        for needed, requested, method in [(True, False, 'POST'), (False, True, 'DELETE'),
            (True, True, None), (False, False, None)]:
            api.reset_mock()
            pr = {'state': 'open', 'requested_teams': [{'slug': atomic_review.TEAM}] if requested else []}
            files = [[{'filename': 'src/include/gcc.h'}]] if needed else [[]]
            api.side_effect = [pr, files, {}]
            atomic_review.run('wiredtiger/wiredtiger', '123')
            self.assertEqual(api.call_count, 3 if method else 2)
            if method:
                api.assert_called_with('repos/wiredtiger/wiredtiger/pulls/123/requested_reviewers',
                    '--method', method, '--input', '-',
                    body={'reviewers': [], 'team_reviewers': [atomic_review.TEAM]})

if __name__ == '__main__':
    unittest.main()
