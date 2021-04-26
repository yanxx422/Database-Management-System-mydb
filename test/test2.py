

'''

CLASSES:
Table
Column
Index





'''


'''
Column_mapping

{attribute: data_type, ...}

'''


import shelve
from blist import sorteddict
import hashlib

class Table:

    def __init__(self, table_name, column_mapping, primary_key):
        self.table_name = table_name
        self.column_mapping = column_mapping
        self.primary_key = primary_key
        self.primary_key_hash_map = shelve.open(table_name + "_primary_key.db")

        # set up the columns list from the column_mapping
        self.columns = [Column(attribute_name, self.column_mapping[attribute_name]) for attribute_name in self.column_mapping.keys()]

        # set up the db file for python shelve
        self.data = shelve.open(table_name + ".db")

        # set up a list of indices
        self.indices = []

    @classmethod
    def create_from_join(cls, left_table, right_table):
        pass
        # joinedTable = cls(table_name, column_mapping)
        #
        # return joinedTable

    def add_record(self, record):
        # --- check if record is legal
        # --------- check if lengths match
        if len(record) != len(self.columns):
            raise Exception("Number of columns in record does not match number of columns in table.")
        # --------- check if data_types match
        for element, column in zip(record, self.columns):
            if not isinstance(element, column.data_type):
                raise Exception("Data_type of record attribute: " + column.attribute_name + " is invalid.")

        # check if record already exists
        hash = self.tuple_hasher(record)
        if hash in self.data:
            raise Exception("Record already in Table. Cannot have duplicate record.")

        # check if record has unique primary key
        indices = []
        for prime in self.primary_key:
            indices.append([x.attribute_name for x in self.columns].index(prime))
        record_list = []
        for index in indices:
            record_list.append(record[index])
        hash2 = self.tuple_hasher(tuple(record_list))

        if hash2 in self.primary_key_hash_map:
            raise Exception("Primary key violation. New record does not have unique primary key.")
        else:
            self.primary_key_hash_map[hash2] = 1

        # add the record to the hash map self.data
        self.data[hash] = record
        print(hash)

    def remove_record(self, record):
        # check if record is present
        hash = self.tuple_hasher(record)

        indices = []
        for prime in self.primary_key:
            indices.append([x.attribute_name for x in self.columns].index(prime))
        record_list = []
        for index in indices:
            record_list.append(record[index])
        hash2 = self.tuple_hasher(tuple(record_list))

        print(hash)
        if hash in self.data:
            print("here")
            del self.data[hash]
            del self.primary_key_hash_map[hash2]
        else:
            raise Exception("Cannot delete non existent record.")

    def update_record(self, columns_to_be_updated, values_to_be_updated, where=None):

        # column_names = [x.attribute_name for x in self.columns]

        indices = []
        for column in columns_to_be_updated:
            indices.append([x.attribute_name for x in self.columns].index(column))

        for record in self._select(where):

            record = list(record)
            # print(record)
            # remove the record in the data
            self.remove_record(record)

            # modify the record
            i = 0
            for index in indices:
                record[index] = values_to_be_updated[i]
                i += 1

            # insert
            self.add_record(tuple(record))

    def tuple_hasher(self, record):
        hash = hashlib.md5()
        for element in record:
            hash.update(str(element).encode())
        return hash.hexdigest()

    def create_index(self, attribute_name):
        try:
            record_pos = [column.attribute_name for column in self.columns].index(attribute_name)
            self.indices.append(Index(attribute_name, record_pos, self.data))
        except:
            print("ERROR: Attribute does not exist. Cannot create index.")

    # def select(self, columns, where=None, order_by=None, group_by=None):
    #     indices = []
    #
    #     for column in columns:
    #         indices.append([x.attribute_name for x in self.columns].index(column))
    #
    #     '''
    #     *********************
    #     *   Attribute_name
    #     *
    #     '''
    #
    #     for key in self.data:
    #         # check where condition
    #         if
    #             toPrint = []
    #         for index in indices:
    #             toPrint.append(self.data[key][index])
    #         yield toPrint


    def _select(self, where=None, order_by=None):

        # if there are no such conditions, simply yield one record at a time
        if where is None and order_by is None:
            for key in self.data:
                yield self.data[key]
            return
        # else there is a condition. An index is needed. Determine if all the indices needed are present
        # check indices for where clause:
        for clause in where:
            if clause["column"] not in [y.attribute_name for y in self.indices]:
                self.create_index(clause["column"])
        # check indices for order_by clause:


        if order_by is None:
            first = 0
            for clause in where:
                if first == 0:
                    operator = clause["symbol"]
                    column = clause["column"]
                    condition = str(clause["condition"])

                    indices_index = [z.attribute_name for z in self.indices].index(column)
                    if operator == "==":
                        try:
                            for key in self.indices[indices_index].data:
                                if key == condition:
                                    for x in self.indices[indices_index].data[key]:
                                        yield self.data[x]
                        except:
                            pass
                    elif operator == "<":
                        try:
                            for key in self.indices[indices_index].data:
                                if key < condition:
                                    for x in self.indices[indices_index].data[key]:
                                        yield self.data[x]
                        except:
                            pass
                    elif operator == ">":
                        try:
                            for key in self.indices[indices_index].data:
                                if key > condition:
                                    for x in self.indices[indices_index].data[key]:
                                        yield self.data[x]
                        except:
                            pass



class Column:

    def __init__(self, attribute_name, data_type):

        self.attribute_name = attribute_name
        self.data_type = data_type

class Index:

    def __init__(self, attribute_name, record_pos, data):
        self.attribute_name = attribute_name
        self.record_pos = record_pos
        self.data = {}

        try:
            for key in data:
                if str(data[key][record_pos]) in self.data:
                    self.data[str(data[key][record_pos])].append(key)
                else:
                    self.data[str(data[key][record_pos])] = [key]
        except:
            pass


#------------------------------------------------------
# PROGRAM STARTS HERE

columns = ["ID", "Name", "Age"]
column_mapping = {"ID": str, "Name": str, "Age": int}
emp1 = ["0001", "Brandon", 25]
emp2 = ("0002", "Michael", 40)
emp3 = ("0003", "Matthew", 25)


t = Table("Employees", column_mapping, ["ID"])

# print(t.tuple_hasher(emp1))
# print(t.tuple_hasher(emp1))


# t.add_record(emp1)
# t.add_record(emp2)
# t.add_record(emp3)
#
# for x in t._select():
#     print(x)
# #
# t.remove_record(emp1)
#
# for x in t._select():
#     print(x)



where = [{"symbol": "<", "column": "Age", "condition": 60}]

# for x in t._select(where):
#     print(x)


t.update_record(["Age"], [20], where)

#
# where = [{"symbol": "==", "column": "Age", "condition": 30}]
#
# for x in t._select(where):
#     print(x)
#
for x in t._select():
    print(x)

s = Table.create_from_join(6, 7)



