import json
import sys
import matplotlib.pyplot as plt


def print_output(output):
    """
    Print output for parsing script
    """
    str = "{"
    for count, key in enumerate(output):
        value = output[key]
        if count:
            str += ", "
        str += "\n\t" + key + ": {\n\t\t\"metadata\": " + json.dumps(value["metadata"]) 
        if "data" in value:
            str += "\n\t\t\"data\": " + json.dumps(value["data"]) 
        if "hs_modify" in value:
            str += "\n\t\t\"hs_modify\": " + json.dumps(value["hs_modify"]) 
        if "hs_update" in value:
            str += "\n\t\t\"hs_update\": " + json.dumps(value["hs_update"]) 
        str += "\n\t}"
    str += "\n}"
    print(str)


def string_to_type(line):
    """
    Helper that converts string to dictionaries and / or lists
    """
    dict = {}
    for x in line.split(" | "):
        kv_pair = x.split(": ", 1)
        if kv_pair[1][0] == "[" and kv_pair[1][-1] == "]":
            kv_pair[1] = kv_pair[1][1:-1]
            kv_pair[1] = kv_pair[1].split(", ")
        if kv_pair[0] == "addr": 
            temp = kv_pair[1][0].split(": ")
            dict.update({"object_id": temp[0], "offset_range": temp[1], "size": kv_pair[1][1], 
                         "checksum": kv_pair[1][2]})
        else:
            dict[kv_pair[0]] = kv_pair[1]
    return dict


def parse_dump_pages(file_path, allow_data, allow_hs):
    """
    Parse the output file of dump_pages
    """
    f = open(file_path, "r")
    line = f.readline() # separator
    output = {}
    cur_node = {"metadata": {}}
    cur_node_id = None
    page_type = None
    time_stamp = None

    while line and line[0:2] != "- ": # checkpoints
        line = f.readline()
    while line:
        if line[0:2] == "- ": # start of a new node
            line = line[len("- "):-1]
            page_info = [line.split(": ")[0]] + line.split(": ")[1].split()
            page_type = page_info[-1]
            if page_info[0] == "Root":
                cur_node_id = page_info[0]
                cur_node["metadata"]["store_type"] = page_info[1]
            elif page_type == "internal":
                output[cur_node_id] = cur_node 
                cur_node_id = line.split(": ")[0]
                cur_node = {"metadata": {}}
            elif page_type == "leaf":
                output[cur_node_id] = cur_node 
                cur_node_id = line.split(": ")[0]
                cur_node = {"metadata": {}}
            else:
                pass
        elif line[0:3] == "\t> ": # metadata for new node
            cur_node["metadata"].update(string_to_type(line[len("\t> "):-1]))
        elif allow_data and (line.startswith("\tK: ") or line.startswith("\trecno: ")): # actual data
            if "data" not in cur_node:
                cur_node["data"] = {}
            key = line[1:-1].split(": ")[1][1:-1]
            metadata = string_to_type(f.readline()[1:-1])
            if page_type == "leaf": 
                value = f.readline()[4:-1][1:-1]
                cur_node["data"][key] = {"value": value, "metadata": metadata}    
            elif page_type == "internal":
                metadata["K"] = key
                cur_node["data"][metadata.pop("ref")] = metadata
        elif allow_hs and line.startswith("\ths_"): # hs info
            [hs_key, hs_value] = line[1:-1].split(": ")
            if hs_key not in cur_node:
                cur_node[hs_key] = []
            cur_node[hs_key].append({hs_value[1:-1]: time_stamp})
            pass
        else: # timestamp 
            time_stamp = string_to_type(line[1:-1])
        line = f.readline() 
    if cur_node_id is not None:
        output[cur_node_id] = cur_node
    f.close()
    return output

