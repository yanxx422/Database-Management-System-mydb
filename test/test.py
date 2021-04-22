## Note the try catch blocks for iterator

path_variable = "/Users/..../mydb/"

from bplustree import BPlusTree, UUIDSerializer
import pickle
import hashlib
import uuid

tree = BPlusTree(path_variable + 'bplustree.db', order=50)
#
# # tree[1] = b'hello'
#
# # tree.insert(2, pickle.dumps(("0001", "Brandon", 25)))
#
# tree.insert(3, pickle.dumps(("0002", "Michael", 40)))
#
# # tree.insert(pickle.dumps(("0002", "Michael", 40)), pickle.dumps(("0002", "Michael", 40)))
#
# print(tree[2])
# print(pickle.loads(tree[2]))
#

# tree.insert(abs(hash("Michael")), pickle.dumps(("0002", "Michael", 40)))

# print(pickle.loads(tree[abs(hash("Michael"))]))

print(tree.get(5))

try:
    for key, value in tree.items():
        print(key, value)
except:
    pass

tree.close()

# print(hash("hello"))

# hasher = hashlib.sha1()
# print(hasher.update(b'hello'))


class Table:

    def __init__(self, table_name, columns):

        self.table_name = table_name

        self.columns = columns

        self.data = BPlusTree('/Users/haddadb1/mydb/' + table_name + ".db", serializer=UUIDSerializer(), key_size=32)
        self.indices = []

    def add_record(self, record):

        # check if record fits columns
        if len(record) == len(self.columns):

            data_key = uuid.uuid1()
            self.data.insert(data_key, pickle.dumps(record))

            # update possible indices
            for index in self.indices:
                index.index_record(record, data_key)

        else:
            print("Error, wrong columns")

    def add_index(self, attribute_name, data):

        # get index of attribute from columns list
        try:
            record_pos = self.columns.index(attribute_name)
            self.indices.append(Index(self.table_name, attribute_name, record_pos, self.data))
        except:
            print("ERROR: Attribute does not exist.")

    def display_all_records(self):
        try:
            for key, value in self.data.items():
                print(key, pickle.loads(value))
        except:
            pass

#     def get_data(self)

class Column:

    def __init__(self, name, data_type):
        self.name = name
        self.data_type = data_type

class Index:

    def __init__(self, table_name, attribute_name, record_pos, data):

        self.record_pos = record_pos
        self.data = BPlusTree(path_variable + table_name + "_" + attribute_name + ".db", serializer=UUIDSerializer(), order=50)

        try:
            for data_key, record in data.items():
                self.index_record(abs(hash(record[record_pos])), data_key)
        except:
            pass

    def index_record(self, record, data_key):
        key = abs(hash(record[record_pos]))
        current = self.data.get(key)
        if current is not None:
            data.insert(key, pickle.dumps([data_key]))
        else:
            current = pickle.loads(current)
            current.append(data_key)
            data[key] = pickle.dumps(current)

    def retrieve_record(self, key_value):

        return pickle.loads(data.get(abs(hash(key_value))))


emp1 = ("0001", "Brandon", 25)
emp2 = ("0002", "Michael", 40)
emp3 = ("0003", "Matthew", 25)

col1 = Column("ID", "string")
col2 = Column("Name", "string")
col3 = Column("Age", "int")

employees = Table("Employees", [col1, col2, col3])

employees.add_record(emp1)

employees.display_all_records()

# print(employees.data.items())
# for key, value in employees.data.items():
#     print(key, value)

employees.data.close()




# key = uuid.uuid1()
# print(key)