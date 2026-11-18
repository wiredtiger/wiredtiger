#!/usr/bin/env python3
#
# Copyright (c) 2014-present MongoDB, Inc.
# Copyright (c) 2008-2014 WiredTiger, Inc.
#    All rights reserved.
#
# See the file LICENSE for redistribution information.

import unittest
from unittest import mock

import atomic_review


class TestAtomicReview(unittest.TestCase):
    def setUp(self):
        self.url = 'https://api.github.com/repos/wiredtiger/wiredtiger/pulls/123'
        self.pr = {'state': 'open', 'requested_teams': []}
        self.files = [{'filename': 'src/btree/bt_read.c', 'patch': '+return (0);'}]
        api_patch = mock.patch.object(atomic_review, 'api')
        self.api = api_patch.start()
        self.addCleanup(api_patch.stop)
        print_patch = mock.patch('builtins.print')
        print_patch.start()
        self.addCleanup(print_patch.stop)

    def test_preserves_manual_review_request(self):
        self.pr['requested_teams'] = [{'slug': atomic_review.TEAM}, {'slug': 'other-team'}]
        self.api.side_effect = [(self.pr, ''), (self.files, ''),
            ({'requested_teams': [{'slug': 'other-team'}]}, '')]

        atomic_review.run(123)

        self.assertEqual(self.api.call_args_list, [mock.call(self.url),
            mock.call(f'{self.url}/files?per_page=100')])
        self.assertEqual(self.pr['requested_teams'],
            [{'slug': atomic_review.TEAM}, {'slug': 'other-team'}])

    def test_requests_review_when_needed(self):
        self.files = [{'filename': 'src/include/gcc.h'}]
        self.api.side_effect = [(self.pr, ''), (self.files, ''),
            ({'requested_teams': [{'slug': atomic_review.TEAM}]}, '')]

        atomic_review.run(123)

        self.assertEqual(self.api.call_args_list, [mock.call(self.url),
            mock.call(f'{self.url}/files?per_page=100'),
            mock.call(f'{self.url}/requested_reviewers',
                body={'reviewers': [], 'team_reviewers': [atomic_review.TEAM]})])

    def test_does_not_duplicate_review_request(self):
        self.pr['requested_teams'] = [{'slug': atomic_review.TEAM}]
        self.files = [{'filename': 'src/include/gcc.h'}]
        self.api.side_effect = [(self.pr, ''), (self.files, '')]

        atomic_review.run(123)

        self.assertEqual(self.api.call_count, 2)

    def test_does_not_request_unneeded_review(self):
        self.api.side_effect = [(self.pr, ''), (self.files, '')]

        atomic_review.run(123)

        self.assertEqual(self.api.call_count, 2)

    def test_requests_review_after_later_atomic_change(self):
        self.api.side_effect = [(self.pr, ''), (self.files, ''), (self.pr, ''),
            ([{'filename': 'src/include/gcc.h'}], ''),
            ({'requested_teams': [{'slug': atomic_review.TEAM}]}, '')]

        atomic_review.run(123)
        self.assertEqual(self.api.call_count, 2)

        atomic_review.run(123)

        self.assertEqual(self.api.call_count, 5)
        self.api.assert_called_with(f'{self.url}/requested_reviewers',
            body={'reviewers': [], 'team_reviewers': [atomic_review.TEAM]})

    def test_checks_all_file_pages(self):
        next_page = f'{self.url}/files?per_page=100&page=2'
        self.api.side_effect = [(self.pr, ''), (self.files, f'<{next_page}>; rel="next"'),
            ([{'filename': 'src/include/hardware.h'}], ''),
            ({'requested_teams': [{'slug': atomic_review.TEAM}]}, '')]

        atomic_review.run(123)

        self.assertEqual(self.api.call_args_list[2], mock.call(next_page))
        self.assertEqual(self.api.call_count, 4)
        self.api.assert_called_with(f'{self.url}/requested_reviewers',
            body={'reviewers': [], 'team_reviewers': [atomic_review.TEAM]})

    def test_skips_closed_pull_request(self):
        self.pr['state'] = 'closed'
        self.api.return_value = (self.pr, '')

        atomic_review.run(123)

        self.api.assert_called_once_with(self.url)

    def test_rejects_unexpected_pull_request_state(self):
        self.pr['state'] = 'unexpected'
        self.api.return_value = (self.pr, '')

        with self.assertRaisesRegex(ValueError, 'Unexpected PR state'):
            atomic_review.run(123)

        self.api.assert_called_once_with(self.url)

    def test_rejects_unapplied_review_request(self):
        self.files = [{'filename': 'src/include/gcc.h'}]
        self.api.side_effect = [(self.pr, ''), (self.files, ''), (self.pr, '')]

        with self.assertRaisesRegex(ValueError, 'GitHub did not apply'):
            atomic_review.run(123)


class TestAtomicReviewAPI(unittest.TestCase):
    def test_get_and_post(self):
        for body, method, status in [(None, 'GET', 200), ({}, 'POST', 201)]:
            with self.subTest(method=method), mock.patch.dict('os.environ',
                {'GITHUB_TOKEN': 'token'}), mock.patch.object(atomic_review, 'urlopen') as urlopen:
                response = urlopen.return_value.__enter__.return_value
                response.status = status
                response.read.return_value = b'{"ok": true}'
                response.headers = {'Link': 'next-page'}

                result, links = atomic_review.api('https://api.github.com', body=body)

                request = urlopen.call_args.args[0]
                self.assertEqual(request.get_method(), method)
                self.assertEqual(request.data, None if body is None else b'{}')
                self.assertEqual(result, {'ok': True})
                self.assertEqual(links, 'next-page')


if __name__ == '__main__':
    unittest.main()
