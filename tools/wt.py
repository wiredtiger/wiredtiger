import pprint
def parse_dump_pages(file_path):
    """
    Parse the output file of dump_pages
    """
    f = open(file_path, "r")
    f.readline() # separator 
    f.readline() # test name
    line = f.readline()
    output = {}
    cur_node = {"metadata": {}}
    cur_node_id = None
    page_type = None
    while line:
        if line[0:2] == "- ":
            page_type = line.split()[-1]
            line = line[len("- "):-1]
            if page_type == "Root":
                cur_node_id = line
            elif page_type == "internal":
                output[cur_node_id] = cur_node 
                cur_node_id = line.split(": ")[0]
                cur_node = {"metadata": {}, "data": {}}
            elif page_type == "leaf":
                output[cur_node_id] = cur_node 
                cur_node_id = line.split(": ")[0]
                cur_node = {"metadata": {}, "data": {}}
            else:
                pass
        elif line[0:3] == "\t> ":
            line = line[len("\t> "):-1]
            for x in line.split(" | "):
                kv_pair = x.split(": ", 1)
                if kv_pair[1][0] == "[" and kv_pair[1][-1] == "]":
                    kv_pair[1] = kv_pair[1][1:-1]
                    kv_pair[1] = kv_pair[1].split(", ")
                cur_node["metadata"][kv_pair[0]] = kv_pair[1]
        elif line.startswith("\tK: ") or line.startswith("\trecno: "):
            if page_type == "leaf": 
                line = line[1:-1]
                key = line.split(": ")[1]
                line = f.readline()[1:-1]
                metadata = {}
                for x in line.split(" | "):
                    kv_pair = x.split(": ", 1)
                    if kv_pair[1][0] == "[" and kv_pair[1][-1] == "]":
                        kv_pair[1] = kv_pair[1][1:-1]
                        kv_pair[1] = kv_pair[1].split(", ")
                    metadata[kv_pair[0]] = kv_pair[1]
                value = f.readline()[4:-2]
                cur_node["data"][key] = {"value": value, "metadata": metadata}    
            elif page_type == "internal":
                line = line[1:-1]
                key = line.split(": ")[1]
                line = f.readline()[1:-1]
                value = {}
                for x in line.split(" | "):
                    kv_pair = x.split(": ", 1)
                    if kv_pair[1][0] == "[" and kv_pair[1][-1] == "]":
                        kv_pair[1] = kv_pair[1][1:-1]
                        kv_pair[1] = kv_pair[1].split(", ")
                    value[kv_pair[0]] = kv_pair[1]
                cur_node["data"][key] = value
        else:
            pass
        line = f.readline() 
    if cur_node_id is not None:
        output[cur_node_id] = cur_node
    pprint.pprint(output)
    f.close()

parse_dump_pages("../WT_TEST/testing/test_row.txt")
