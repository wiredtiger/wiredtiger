#!/usr/bin/env python

import wttest
from helper_tiered import get_auth_token, TieredConfigMixin
from wtscenario import make_scenarios

class test_tiered06(wttest.WiredTigerTestCase, TieredConfigMixin):

    tiered_storage_sources = [
        ('azure_store', dict(is_tiered = True,
            is_local_storage = False,
            auth_token = get_auth_token('azure_store'), 
            bucket = 'pythontest',
            bucket_prefix = "pfx_",
            ss_name = 'azure_store')),
        ('gcp_store', dict(is_tiered = True,
            is_local_storage = False,
            auth_token = get_auth_token('gcp_store'), 
            bucket = 'pythontest',
            bucket_prefix = "pfx_",
            ss_name = 'gcp_store')),
    ]

    # Make scenarios for different cloud service providers
    scenarios = make_scenarios(tiered_storage_sources)
    
    def conn_extensions(self, extlist):
        TieredConfigMixin.conn_extensions(self, extlist)

    def test_gcp_and_azure(self): 
        pass
