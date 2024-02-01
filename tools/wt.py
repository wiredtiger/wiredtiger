import json
import sys
import matplotlib.pyplot as plt


def print_output(output):
    """
    Print output for parsing script
    """
    str = "{\n\t"
    for checkpoint in output:
        str += "{ " + checkpoint + ": "
        for count, key in enumerate(output[checkpoint]):
            value = output[checkpoint][key]
            if count:
                str += ", "
            str += "\n\t\t" + key + ": {\n\t\t\t\"metadata\": " + json.dumps(value["metadata"]) 
            if "data" in value:
                str += "\n\t\t\t\"data\": " + json.dumps(value["data"]) 
            if "hs_modify" in value:
                str += "\n\t\t\t\"hs_modify\": " + json.dumps(value["hs_modify"]) 
            if "hs_update" in value:
                str += "\n\t\t\t\"hs_update\": " + json.dumps(value["hs_update"]) 
            str += "\n\t\t}"
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


def parse_dump_pages(file_path):
    """
    Parse the output file of dump_pages
    """
    separator = "=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=\n"
    f = open(file_path, "r")
    output = {}
    checkpoint = {}
    line = f.readline() # separator
    cur_node = {"metadata": {}}
    cur_node_id = None
    page_type = None

    while line:
        if line == separator:
            line = f.readline() # checkpoint
            checkpoint_name = line.split(", ")[-1].split(": ")[-1][:-1]
            output[checkpoint_name] = checkpoint
        while line != separator and line:
            if line[0:2] == "- ": # start of a new node
                line = line[len("- "):-1]
                page_info = [line.split(": ")[0]] + line.split(": ")[1].split()
                page_type = page_info[-1]
                if page_info[0] == "Root":
                    cur_node_id = page_info[0]
                    cur_node["metadata"]["store_type"] = page_info[1]
                elif page_type == "internal" or page_type == "leaf":
                    checkpoint[cur_node_id] = cur_node 
                    cur_node_id = line.split(": ")[0]
                    cur_node = {"metadata": {}}
                else:
                    pass
            elif line[0:3] == "\t> ": # metadata for new node
                cur_node["metadata"].update(string_to_type(line[len("\t> "):-1]))
            line = f.readline() 
        if cur_node_id is not None:
            checkpoint[cur_node_id] = cur_node
    f.close()
    return output


def visualise(output, field, saved_path = "./"):
    values = []
    keys = []
    all = list(output.keys())
    for count, val in enumerate(output.values()):
        if field in val["metadata"]:
            values.append(val["metadata"][field])
            keys.append(all[count])
    plt.figure(figsize=(30,6))
    plt.hist(values, bins=60, color='g', alpha=0.7)
    plt.title('Histogram of ' + field + ' in Megabytes (MB)')
    plt.xlabel(field + ' (MB)')
    plt.ylabel('Frequency')
    plt.savefig(saved_path + field)
    plt.close()
    

def main():
    file = sys.argv[1]
    output = parse_dump_pages(file)
    # visualise(output, "dsk_mem_size", "../WT_TEST/plot_images/")
    print_output(output)

if __name__ == '__main__':
    main()
