import gdb, sys
 
f = open("dump.txt", "w")
inserts = {}
 
# This script walks an insert list in WiredTiger, it does so by traversing the bottom level of the
# skip list and printing the next pointer array. It dumps the data to "dump.txt" as the content
# is often too large for the terminal.
#
# It takes a WT_INSERT_HEAD structure and a key format. In order to use it in gdb it must first be
# sourced. Example usage:
# source(dump_insert.py)
# insert_list_dump WT_INSERT_HEAD,S
#
# It supports 3 key formats: u, i, S.
class insert():
    def __init__(self, key, address, next_array):
        self.key = key
        self.address = address
        self.next_array = next_array
 
    def print(self):
        f.write("Key: " + str(self.key) + " Address: " + self.address + " Next array: " + str(self.next_array) + "\n")
 
class insert_list_dump(gdb.Command):
    key_format = 'S'
    def __init__(self):
        super(insert_list_dump, self).__init__("insert_list_dump", gdb.COMMAND_DATA)

    def usage(self):
        print("usage:")
        print("insert_list_dump WT_INSERT_HEAD,key_format")
        exit(1)

    def decode_key(self, key):
        if self.key_format == 'S':
            return(key.decode())
        elif self.key_format == 'u':
            return(key)
        elif self.key_format == 'i':
            return(key)
        else:
            print("Invalid key format supplied.")
            self.usage()
        
    def get_key(self, insert):
        key_struct = insert['u']['key']
        key = gdb.selected_inferior().read_memory(int(insert) + key_struct['offset'], key_struct['size']).tobytes()
        decoded_key = self.decode_key(key)
        return decoded_key
 
    def walk_level(self, head, id):
        current = head[0]
        while current != 0x0:
            key = self.get_key(current)
            next = []
            for i in range(0, 10):
                next.append(str(current['next'][i]))
                if (str(current['next'][i]) == "0x0"):
                    break
            inserts.update({key : insert(key, str(current), next)})
            inserts[key].print()
            current = current['next'][0]
 
    def invoke(self, args, from_tty):
        arg_array = args.split(',')
        self.key_format = arg_array[1].strip()
        print("Parsing the passed in WT_INSERT_HEAD...")
        wt_insert_head = gdb.parse_and_eval(arg_array[0])
        print("Walking the insert list...")
        self.walk_level(wt_insert_head['head'], 0)
        print("Complete...")
 
# This registers our class to the gdb runtime at "source" time.
insert_list_dump()
