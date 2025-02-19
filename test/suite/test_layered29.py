import wttest
from helper_disagg import DisaggConfigMixin, disagg_test_class, gen_disagg_storages

# test_layered29.py
# Test we can create a large number of layared tables
@disagg_test_class
class test_layered29(wttest.WiredTigerTestCase, DisaggConfigMixin):
    conn_base_config = ',create,statistics=(all),statistics_log=(wait=1,json=true,on_close=true),' \
                 + 'disaggregated=(page_log=palm),'
    def conn_config(self):
        return self.extensionsConfig() + self.conn_base_config + 'disaggregated=(role="leader")'

    scenarios = gen_disagg_storages('test_layered29', disagg_only = True)

    def test_create_tables(self):
        for i in (0, 10000):
            self.assertEquals(self.session.create("layered:test_table" + str(i), "key_format=S,value_format=S"), 0)
