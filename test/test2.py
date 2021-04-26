

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
        print("indices", indices)
        record_list = []
        for index in indices:
            record_list.append(record[index])
        print("here", record_list)
        hash = self.tuple_hasher(tuple(record_list))

        if hash in self.primary_key_hash_map:
            raise Exception("Primary key violation. New record does not have unique primary key.")
        else:
            self.primary_key_hash_map[hash] = 1

        # add the record to the hash map self.data
        self.data[hash] = record

    def remove_record(self, record):
        # check if record is present
        hash = self.tuple_hasher(record)
        if hash in self.data:
            del self.data[hash]
        else:
            raise Exception("Cannot delete non existent record.")

    # def update_record(self, ):

    def tuple_hasher(self, record):
        hash = hashlib.md5()
        for element in record:
            hash.update(str(element).encode())
        return hash.hexdigest()


    def select(self, columns):

        indices = []

        for column in columns:
            indices.append([x.attribute_name for x in self.columns].index(column))


        '''
        *********************
        *   Attribute_name
        *
        '''

        for key in self.data:
            toPrint = []
            for index in indices:
                toPrint.append(self.data[key][index])
            print(toPrint)


class Column:

    def __init__(self, attribute_name, data_type):

        self.attribute_name = attribute_name
        self.data_type = data_type


#------------------------------------------------------
# PROGRAM STARTS HERE

columns = ["ID", "Name", "Age"]
column_mapping = {"ID": str, "Name": str, "Age": int}
emp1 = ("0001", "Brandon", 25)
emp2 = ("0003", "Michael", 40)
emp3 = ("0003", "Matthew", 25)


t = Table("Employees", column_mapping, ["ID"])
t.add_record(emp2)
# t.remove_record(emp1)
t.select(columns)

s = Table.create_from_join(6, 7)



