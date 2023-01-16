#!/usr/bin/env python

import inspect, os, wiredtiger, wttest
from helper_tiered import TieredConfigMixin, gen_tiered_storage_sources

FileSystem = wiredtiger.FileSystem  # easy access to constants

class test_tiered06(wttest.WiredTigerTestCase, TieredConfigMixin):

    def conn_extensions(self, extlist):
        TieredConfigMixin.conn_extensions(self, extlist)

    def test_azure_and_gcp(self): 
        storage_sources = gen_tiered_storage_sources(wttest.getss_random_prefix(), 'test_tiered06', tiered_only=True)