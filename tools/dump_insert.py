import gdb

inserts = {}

class insert():
    def __init__(self, key, address, next_array):
        self.key = key
        self.address = address
        self.next_array = next_array

    def print(self):
        print("Key : " + self.key + " Address: " + self.address + " Next array: " + str(self.next_array))

class insert_list_dump(gdb.Command):
    def __init__(self):
        super(insert_list_dump, self).__init__("insert_list_dump", gdb.COMMAND_DATA)

    def get_key(self, insert):
        key_struct = insert['u']['key']
        key = gdb.selected_inferior().read_memory(int(insert) + key_struct['offset'], key_struct['size']).tobytes().replace(b'\x00',b'').decode()
        return key.strip('0')

    def walk_level(self, head, id):
        #print(start.dereference())
        current = head['head'][0]
        while current != 0x0:
            key = self.get_key(current)
            next = []
            for i in range(0,10):
                next.append(str(current['next'][i]))
            inserts.update({key : insert(key, str(current), next)})
            inserts[key].print()
            current = current['next'][0]

    def invoke(self, insert_head, from_tty):
        head = gdb.parse_and_eval(insert_head).dereference().dereference()
        print(head)
        self.walk_level(head, 0)
        

# This registers our class to the gdb runtime at "source" time.
insert_list_dump()

