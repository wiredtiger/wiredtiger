# Create tables that are used by our doxygen filter. to process and
# expand the arch_page macros in the documentation.

class ArchDocPage:
    def __init__(self, doxygen_name, data_structures, files):
        self.doxygen_name = doxygen_name
        self.data_structures = data_structures
        self.files = files

##########################################
# List of all architecture subsections
##########################################
arch_doc_pages = [
    ArchDocPage('arch-schema',
        ['WT_TABLE', 'WT_COLGROUP', 'WT_INDEX'],
        ['src/include/schema.h', 'src/include/intpack_inline.h',
         'src/include/packing_inline.h', 'src/schema/', 'src/packing/']),
    ArchDocPage('arch-dhandle',
        ['WT_DHANDLE', 'WT_BTREE'],
        ['src/include/dhandle.h', 'src/include/btree.h',
         'src/conn/conn_dhandle.c', 'src/session/session_dhandle.c']),
    ArchDocPage('arch-transaction',
        ['WT_TXN', 'WT_TXN_SHARED', 'WT_TXN_GLOBAL', 'WT_TXN_OP'],
        ['src/include/txn.h', 'src/txn/']),
]
