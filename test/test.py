## Note the try catch blocks for iterator

path_variable = "/Users/haddadb1/mydb/"

from bplustree import BPlusTree, UUIDSerializer
import pickle
import hashlib
import uuid
from enum import Enum

# tree = BPlusTree(path_variable + 'bplustree.db', order=50)
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

# tree.insert(18000000000000000001, pickle.dumps(("0002", "Michael", 40)))


# print(pickle.loads(tree[abs(hash("Michael"))]))

# print(tree.get(5))

# try:
#     for key, value in tree.items():
#         print(key, value)
# except:
#     pass

# tree.close()

# print(hash("hello"))

# hasher = hashlib.sha1()
# print(hasher.update(b'hello'))

#------------------------------------------------------------------------------
varia = uuid.uuid1()
print(varia, type(varia))

newTree = BPlusTree('/Users/haddadb1/mydb/example.db', serializer=UUIDSerializer(), key_size=32, order=100, page_size=128, cache_size=4096)

the  = "1!!!!!!!!!!!!!!!"
# the2 = "1.3aaaaaaaaaaaaa"
print("here", uuid.UUID(bytes=the.encode('utf-8')))
print(the.encode('utf-8'))
print(bytes(the, 'utf-8'))

# newTree.insert(uuid.UUID(bytes=the.encode('utf-8')), pickle.dumps("1new"))
# newTree.insert(uuid.UUID(bytes=the2.encode('utf-8')), pickle.dumps("1.3"))
# newTree.remove(uuid.UUID(bytes=the.encode('utf-8')), pickle.dumps("1new"))
# print(dir(newTree))

# print(newTree[uuid.UUID(bytes=the.encode('utf-8'))])
#
# for i in range(1, 2000):
#     newTree.insert(uuid.uuid1(), pickle.dumps("hello"))
#     newTree.checkpoint()

print(newTree.keys())

# del newTree[uuid.UUID(bytes=the.encode('utf-8'))]

print("------->", type(1.3))

print(str(1.2) + "a")

try:
    for key, value in newTree.items():
        print(key, pickle.loads(value))
except:
    pass

newTree.close()
#------------------------------------------------------------------------------

# class Table:
#
#     def __init__(self, table_name, columns):
#
#         self.table_name = table_name
#
#         self.columns = columns
#
#         self.data = BPlusTree('/Users/haddadb1/mydb/' + table_name + ".db", serializer=UUIDSerializer(), key_size=32)
#         self.indices = []
#
#     def add_record(self, record):
#
#         # check if record fits columns
#         if len(record) == len(self.columns):
#
#             data_key = uuid.uuid1()
#             self.data.insert(data_key, pickle.dumps(record))
#
#             # update possible indices
#             for index in self.indices:
#                 index.index_record(record, data_key)
#
#         else:
#             print("Error, wrong columns")
#
#     def add_index(self, attribute_name):
#
#         # get index of attribute from columns list
#         try:
#             col_names = [column.name for column in self.columns]
#             record_pos = col_names.index(attribute_name)
#             print(col_names)
#             print(self.columns)
#             print(record_pos)
#             self.indices.append(Index(self.table_name, attribute_name, record_pos, self.data))
#         except:
#             print("ERROR: Attribute does not exist.")
#
#     def display_all_records(self):
#         try:
#             for key, value in self.data.items():
#                 print(key, pickle.loads(value))
#         except:
#             pass
#
#     def close_db_files(self):
#
#         # iterate over indices list and close btree db files
#         for index in self.indices:
#             index.data.close()
#
#         # close the main data db file of the table object
#         self.data.close()
#
# #     def get_data(self)
#
# class Column:
#
#     def __init__(self, name, data_type):
#         self.name = name
#         self.data_type = data_type
#         self.constraints = []
#
#     def isUnique(self):
#         pass
#
#     def isPrimaryKey(self):
# #         check if primary key is contained in constraints
# #         if contained in constraints, check if record[attribute] is unique
#         pass
# #         self.isUnique
#
# class Index:
#
#     def __init__(self, table_name, attribute_name, record_pos, data):
#
#         self.attribute_name = attribute_name
#         self.record_pos = record_pos
#         self.data = BPlusTree(path_variable + table_name + "_" + attribute_name + ".db", serializer=UUIDSerializer(), order=50)
#
#         try:
#             for data_key, record in data.items():
#                 self.index_record(abs(hash(record[record_pos])), data_key)
#         except:
#             pass
#
#     def index_record(self, record, data_key):
#         key = abs(hash(record[record_pos]))
#         current = self.data.get(key)
#         if current is not None:
#             data.insert(key, pickle.dumps([data_key]))
#         else:
#             current = pickle.loads(current)
#             current.append(data_key)
#             data[key] = pickle.dumps(current)
#
#     def retrieve_records_list(self, attribute_value):
#         return pickle.loads(data.get(abs(hash(attribute_value))))
#
#
# class data_type(Enum):
#     INTEGER = 1
#     FLOAT = 2
#     VARCHAR = 3
#
# class constraint(Enum):
#     PRIMARY = 1
#     UNIQUE = 2
#     NULL = 3
#     NOT_NULL = 4
#
#
# # class Interpreter:
# #
# #     __init__(self):
# #         table_list = []
# #
# #
# #
# #         while
# #             prompt
# #             receive string command
# #
# #             self.interpret(string command)
#
#
#
#
#
#
#
#
# emp1 = ("0001", "Brandon", 25)
# emp2 = ("0002", "Michael", 40)
# emp3 = ("0003", "Matthew", 25)
#
# col1 = Column("ID", data_type.VARCHAR)
# col2 = Column("Name", data_type.VARCHAR)
# col3 = Column("Age", data_type.INTEGER)
#
# employees = Table("Employees", [col1, col2, col3])
#
# # employees.add_record(emp1)
#
# # employees.display_all_records()
#
# employees.add_index("Name")
#
# # print(employees.data.items())
# # for key, value in employees.data.items():
# #     print(key, value)
#
# employees.close_db_files()
#

#
# print(hash("hello"))
# dig = hashlib.md5(str(35).encode()).digest()
#
# print(dig)
#
# # the = uuid.UUID(bytes=dig.encode('utf-8'))
#
# the = uuid.UUID(bytes=dig)
#
# print(the)






#-------------------------------------------------------
# tuple = ("0001", "Brandon", 25)
#
# hash = hashlib.md5()
#
# for element in tuple:
#     hash.update(str(element).encode())
#
# data_key = uuid.UUID(bytes=hash.digest())
#
# print(data_key)
# -----------------------------------------------------








# import hashlib
#
# t = ('x', 'y', 'z')
#
# m = hashlib.md5()
# for s in t:
#     m.update(s.encode())
# fn = m.hexdigest() # => 'd16fb36f0911f878998c136191af705e'


# key = uuid.uuid1()
# print(key)