# Read and verify the documentation data to make sure path names are valid.

import os
import docs_data

top_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
for page in docs_data.arch_doc_pages:
    name = page.doxygen_name
    for partial in page.files:
        fullpath = os.path.join(top_dir, partial)
        if not os.path.exists(fullpath):
            print(name + ': ' + partial + ': does not exist')
        elif os.path.isdir(fullpath):
            if fullpath[-1:] != '/':
                print(name + ': ' + partial + ': is a directory, must end in /')
        else:
            if fullpath[-1:] == '/':
                print(name + ': ' + partial + ': not a directory, cannot end in /')
