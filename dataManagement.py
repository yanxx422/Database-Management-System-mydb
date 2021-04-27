

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
import os

from enum import Enum


class ColumnType(Enum):
    INT = int = 'int'
    VARCHAR = varchar = 'str'
    FLOAT = float = 'float'


TYPE_MAP = {
    'int': int,
    'float': float,
    'str': str,
    'INT': int,
    'FLOAT': float,
    'VARCHAR': str,
    'varchar': str
}



class Table:

    def __init__(self, table_name, column_mapping, primary_key):

        # check if user has provided valid primary key list
        if len(primary_key) < 1:
            raise Exception("You must specify the primary key column.")

        # Check if column_types are valid
        # Check if column_types are valid
        for key, value in column_mapping.items():
            if not (value in ColumnType.__members__):
                raise TypeError('Data type is not valid')

        # for i in range(len(column_names)):
        #     if not (column_types[i] in ColumnType.__members__):
        #         raise TypeError('Data type is not valid')

        # Check if column names are unique:
        if len(set(column_mapping.keys())) != len(column_mapping.keys()):
            raise Exception("Can't have duplicate column names.")

        # if len(set(column_names)) != len(column_names):
        #     raise Exception("Can't have duplicate column names.")

        # check that primary key columns are actually columns of the table
        if not any(elem in primary_key for elem in column_mapping.keys()):
            raise Exception("primary_key '%s' does not exist." % primary_key)

        # # check if user has provided valid primary key list
        # if len(primary_key) < 1:
        #     raise Exception("You must specify the primary key column.")
        #
        # # Check if column_types are valid
        # for key in column_mapping:
        #     if column_mapping[key] is not int and column_mapping[key] is not float and column_mapping[key] is not str:
        #         raise TypeError('Data type is not valid')
        #
        # # for i in range(len(column_names)):
        # #     if not (column_types[i] in ColumnType.__members__):
        # #         raise TypeError('Data type is not valid')
        #
        # # Check if column names are unique:
        # if len(set(column_mapping.keys())) != len(column_mapping.keys()):
        #     raise Exception("Can't have duplicate column names.")
        #
        # # if len(set(column_names)) != len(column_names):
        # #     raise Exception("Can't have duplicate column names.")
        #
        # # check that primary key columns are actually columns of the table
        # if not any(elem in primary_key for elem in column_mapping.keys()):
        #     raise Exception("primary_key '%s' does not exist." % primary_key)
        #
        # # # check if table name exists
        # # for key, value in tables.items():
        # #     if table_name == key:
        # #         raise Exception("Can't have duplicate column names.")
        #

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

        # set up a size variable, relevant for joins and other efficiency optimizations.
        self.size = 0

    @classmethod
    def create_from_join(cls, left_table, right_table, leftColumnOfJoin, rightColumnOfJoin):


        # Check if the columns to be joined on match have the same data type
        # get left table's column object of join
        left_table_join_column = None
        right_table_join_column = None

        for x in left_table.columns:
            if x.attribute_name == leftColumnOfJoin:
                left_table_join_column = x

        for x in right_table.columns:
            if x.attribute_name == leftColumnOfJoin:
                right_table_join_column = x

        if left_table_join_column is None:
            raise Exception("Error, cannot join on non-existent left table column. Check column name.")
        if right_table_join_column is None:
            raise Exception("Error, cannot join on non-existent right table column. Check column name.")

        if left_table_join_column.data_type is not right_table_join_column.data_type:
            raise Exception("Error, cannot join columns of different type.")

        # Determine smaller and larger table
        smaller_table = left_table
        smaller_column_of_join = leftColumnOfJoin
        larger_table = right_table
        larger_column_of_join = rightColumnOfJoin

        if left_table.size > right_table.size:
            smaller_table = right_table
            smaller_column_of_join = rightColumnOfJoin
            larger_table = left_table
            larger_column_of_join = leftColumnOfJoin

        # confirm existence or create index on join column of larger table
        if larger_column_of_join not in [index.attribute_name for index in larger_table.indices]:
            larger_table.create_index(larger_column_of_join)

        # create the column mapping of the joined table
        mapping = {}
        for key in smaller_table.column_mapping:
            mapping[smaller_table.table_name + "." + key] = smaller_table.column_mapping[key]

        for key in larger_table.column_mapping:
            if key is not larger_column_of_join:
                mapping[larger_table.table_name + "." + key] = larger_table.column_mapping[key]

        # print("MAPPING", mapping)

        # modifying primary keys in primary keys list to add table_name of smaller table
        joined_primary_keys = []
        for primary in smaller_table.primary_key:
            joined_primary_keys.append(smaller_table.table_name + "." + primary)

        # instantiate the table object
        joined_table = cls("joined_table", mapping, joined_primary_keys)

        # get position of joining column in both tables
        smaller_table_column_index = [column.attribute_name for column in smaller_table.columns].index(smaller_column_of_join)
        larger_table_column_index = [column.attribute_name for column in larger_table.columns].index(larger_column_of_join)

        # get position of joining column index in larger table
        larger_table_index_position = [index.attribute_name for index in larger_table.indices].index(larger_column_of_join)

        # load the table with the joined records
        for record in smaller_table._select():
            # gather the records in the larger table that match, via indexing
            other_record_list_id = larger_table.indices[larger_table_index_position].data[record[smaller_table_column_index]]
            other_records_list_tuples = []
            for id in other_record_list_id:
                other_records_list_tuples.append(larger_table.data[id])

            for other_record in other_records_list_tuples:

                joined_record = list(record)

                for i in range(0, len(other_record)):
                    if i is not larger_table_column_index:
                        joined_record.append(other_record[i])

                joined_record = tuple(joined_record)

                joined_table.add_record(joined_record)


        return joined_table


        # joinedTable = cls(table_name, column_mapping)
        #
        # return joinedTable

    def drop_table(self):
        self.data.close()
        self.primary_key_hash_map.close()

        os.remove(self.table_name + ".db")
        os.remove(self.table_name + "_primary_key.db")

    def add_record(self, record):

        # --- check if record is legal
        # --------- check if lengths match
        if len(record) != len(self.columns):
            raise Exception("Number of columns in record does not match number of columns in table.")
        # --------- check if data_types match
        for element, column in zip(record, self.columns):
            if not isinstance(element, TYPE_MAP[column.data_type]):
                raise TypeError('data type error, value must be %s' % column.data_type)
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
        # print(hash)

        self.size += 1

        # # --- check if record is legal
        # # --------- check if lengths match
        # if len(record) != len(self.columns):
        #     raise Exception("Number of columns in record does not match number of columns in table.")
        # # --------- check if data_types match
        # for element, column in zip(record, self.columns):
        #     if not isinstance(element, column.data_type):
        #         raise Exception("Data_type of record attribute: " + column.attribute_name + " is invalid.")
        #
        # # check if record already exists
        # hash = self.tuple_hasher(record)
        # if hash in self.data:
        #     raise Exception("Record already in Table. Cannot have duplicate record.")
        #
        # # check if record has unique primary key
        # indices = []
        # for prime in self.primary_key:
        #     indices.append([x.attribute_name for x in self.columns].index(prime))
        # record_list = []
        # for index in indices:
        #     record_list.append(record[index])
        # hash2 = self.tuple_hasher(tuple(record_list))
        #
        # if hash2 in self.primary_key_hash_map:
        #     raise Exception("Primary key violation. New record does not have unique primary key.")
        # else:
        #     self.primary_key_hash_map[hash2] = 1
        #
        # # add the record to the hash map self.data
        # self.data[hash] = record
        # # print(hash)
        #
        # self.size += 1

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

        self.size -= 1

    def remove_where(self, where):
        for x in self._select_filter(where):
            self.remove_record(x)

    def update_record(self, columns_to_be_updated, values_to_be_updated, where=None):

        # column_names = [x.attribute_name for x in self.columns]

        indices = []
        for column in columns_to_be_updated:
            indices.append([x.attribute_name for x in self.columns].index(column))

        for record in self._select_filter(where):

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

    def drop_index(self, column_name):

        try:
            index = [x.attribute_name for x in self.indices].index(column_name)
            del self.indices[index]
        except:
            raise Exception("Cannot drop non existent index.")

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


    # def _select(self, where=None, order_by=None):
    #
    #     # if there are no such conditions, simply yield one record at a time
    #     if where is None and order_by is None:
    #         for key in self.data:
    #             yield self.data[key]
    #         return
    #     # else there is a condition. An index is needed. Determine if all the indices needed are present
    #     # check indices for where clause:
    #     for clause in where:
    #         if clause["column"] not in [y.attribute_name for y in self.indices]:
    #             self.create_index(clause["column"])
    #     # check indices for order_by clause:
    #
    #
    #     if order_by is None:
    #         first = 0
    #         for clause in where:
    #             if first == 0:
    #                 operator = clause["symbol"]
    #                 column = clause["column"]
    #                 condition = str(clause["condition"])
    #
    #                 indices_index = [z.attribute_name for z in self.indices].index(column)
    #                 if operator == "==":
    #                     try:
    #                         for key in self.indices[indices_index].data:
    #                             if key == condition:
    #                                 for x in self.indices[indices_index].data[key]:
    #                                     yield self.data[x]
    #                     except:
    #                         pass
    #                 elif operator == "<":
    #                     try:
    #                         for key in self.indices[indices_index].data:
    #                             if key < condition:
    #                                 for x in self.indices[indices_index].data[key]:
    #                                     yield self.data[x]
    #                     except:
    #                         pass
    #                 elif operator == ">":
    #                     try:
    #                         for key in self.indices[indices_index].data:
    #                             if key > condition:
    #                                 for x in self.indices[indices_index].data[key]:
    #                                     yield self.data[x]
    #                     except:
    #                         pass

    def aggregation(self, operation_type, record_data, current):
        if operation_type == "sum":
            if current == "a":
                return record_data
            else:
                return current + record_data
        elif operation_type == "avg":
            if current == "a":
                return record_data
            else:
                return current + record_data
        elif operation_type == "count":
            if current == "a":
                return 1
            else:
                return current + 1
        elif operation_type == "min":
            if current == "a":
                return record_data
            if record_data < current:
                return record_data
            else:
                return current
        elif operation_type == "max":
            if current == "a":
                return record_data
            if record_data > current:
                return record_data
            else:
                return current


    def select(self, columns, aggregates=None, group_by=None, where=None, order_by=None, direction="desc"):

        # this is the function were the code lost its structure completely
        if aggregates is not None:

            order_by_cols = []
            for ag in aggregates:
                print(ag[0])
                order_by_cols.append(ag[0])

            print(order_by_cols)
            # order_by = order_by_cols

            toPrint = []
            column_indices = []
            # get column indices
            for column in columns:
                column_indices.append([x.attribute_name for x in self.columns].index(column))

            # get mapping from index to column
            index_to_columns = {}
            column_to_index = {}
            i = 0
            for index in column_indices:
                index_to_columns[index] = columns[i]
                column_to_index[columns[i]] = index
                i += 1

            # get mapping from index to aggregate function
            index_to_aggregate_function = {}
            for aggregate in aggregates:
                index_to_aggregate_function[column_to_index[aggregate[0]]] = aggregate[1]

            list_of_results = []
            # get mapping from index to integer
            index_to_var = {}
            for index in index_to_aggregate_function:
                index_to_var[index] = "a"

            if group_by is not None:
                change = False

                # get group_by indices
                group_by_column_indices = []
                for column in group_by:
                    group_by_column_indices.append([x.attribute_name for x in self.columns].index(column))

                # set up mapping from group by indices to state values
                group_by_index_mapping = {}
                for index in group_by_column_indices:
                    group_by_index_mapping[index] = 0

                hashPrior = self.tuple_hasher(group_by_index_mapping.values())
                hashCurrent = self.tuple_hasher(group_by_index_mapping.values())

                for record in self._select_filter(where, order_by_cols, direction):
                    for index in index_to_var:
                        index_to_var[index] = self.aggregation(index_to_aggregate_function[index], record[index], index_to_var[index])

                    for key in group_by_index_mapping:
                        group_by_index_mapping[key] = record[key]

                    hashPrior = hashCurrent
                    hashCurrent = self.tuple_hasher(group_by_index_mapping.values())

                    if hashPrior != hashCurrent:
                        change = True

                    if group_by is None:
                        change = False

                    if change is True:
                        list_of_results.append(index_to_var)
                        index_to_var = {}
                        for index in index_to_aggregate_function:
                            index_to_var[index] = "a"
                        change = False
            else:
                for record in self._select_filter(where, order_by, direction):
                    for index in index_to_var:
                        index_to_var[index] = self.aggregation(index_to_aggregate_function[index], record[index], index_to_var[index])
                list_of_results.append(index_to_var)
            # # get mapping from index to integer
                # index_to_var = {}
                # for index in index_to_aggregate_function:
                #     index_to_var[index] = "a"
                #
                #
                # for record in self._select_filter(where, order_by, direction):
                #     for index in index_to_var:
                #         index_to_var[index] = self.aggregation(index_to_aggregate_function[index], record[index], index_to_var[index])

            names = []
            for name in aggregates:
                names.append(name[1] + "(" + name[0] + ")")
            print(names)
            for result in list_of_results:
                print(result)
        else:

            for x in self._select_filter(where, order_by, direction):
                print(x)


    def _filter(self, record, where):

        operator = where["symbol"]
        column = where["column"]
        condition = str(where["condition"])

        # get index of column to check
        column_index = [x.attribute_name for x in self.columns].index(column)

        if operator == "==":
            if str(record[column_index]) == condition:
                return True
            else:
                return False
        elif operator == "<":
            if str(record[column_index]) < condition:
                return True
            else:
                return False
        elif operator == ">":
            if str(record[column_index]) > condition:
                return True
            else:
                return False
        elif operator == "<>":
            if str(record[column_index]) != condition:
                return True
            else:
                return False

    def _select_filter(self, where=None, order_by=None, direction="desc"):

        # if there are no such conditions, simply yield one record at a time
        if where is None and order_by is None:
            for key in self.data:
                yield self.data[key]
            return


        if order_by is None:
            # if there is more than one where, the outer wheres will be processed linearly.
            # it does not make sense to make this unique combination of wheres into an index
            if len(where) > 1:
                for x in self._select_filter(where[1:]):
                    print("--->", where[0])
                    if self._filter(x, where[0]):
                        yield x
                return
            else:
                print(where)
                operator = where[0]["symbol"]
                column = where[0]["column"]
                condition = str(where[0]["condition"])
                print(column)

                # if the index does not exit, create it
                if column not in [y.attribute_name for y in self.indices]:
                    self.create_index(column)

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
                return
        elif where is None:
            for x in self._select_sort(order_by):
                yield x
        else:
            if len(where) > 1:
                for x in self._select_filter(where[1:], order_by, direction):
                    if self._filter(x, where[0]):
                        yield x
                return
            else:
                for x in self._select_sort(order_by, direction):
                    if self._filter(x, where[0]):
                        yield x
                return


    def _select_sort(self, order_by, direction="desc"):

        if len(order_by) > 1:
            newIndex = self._create_index_from_generator(self._select_sort(order_by[1:], direction), order_by[0])
            keys = list(newIndex.keys())

            if direction == "asc":
                keys.sort()
            elif direction == "desc":
                keys.sort(reverse=True)

            for key in keys:
                for record in newIndex[key]:
                    # print(">>>>>>>>>", key, newIndex[key], record)
                    yield self.data[record]
        else:

            column = order_by[0]

            # if the index does not exit, create it
            if column not in [y.attribute_name for y in self.indices]:
                self.create_index(column)

            index_pos = [x.attribute_name for x in self.indices].index(column)

            keys = list(self.indices[index_pos].data.keys())

            if direction == "asc":
                keys.sort()
            elif direction == "desc":
                keys.sort(reverse=True)

            for key in keys:
                for record in self.indices[index_pos].data[key]:
                    yield self.data[record]

    def _create_index_from_generator(self, records, column):


        index = {}

        for x in [x.attribute_name for x in self.columns]:
            print(x)

        column_index = [x.attribute_name for x in self.columns].index(column)

        try:
            for record in records:
                print("heree", record[column_index], index.keys())
                if str(record[column_index]) in index:
                    print("heree", record[column_index])
                    index[str(record[column_index])].append(self.tuple_hasher(record))
                else:
                    index[str(record[column_index])] = [self.tuple_hasher(record)]

        except:
            pass

        return index
        # try:
        #     for key in data:
        #         if str(data[key][record_pos]) in self.data:
        #             self.data[str(data[key][record_pos])].append(key)
        #         else:
        #             self.data[str(data[key][record_pos])] = [key]
        # except:
        #     pass

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
#
# columns = ["ID", "Name", "Age"]
# column_mapping = {"ID": "VARCHAR", "Name": "VARCHAR", "Age": "int"}
# emp1 = ["0001", "Brandon", 25]
# emp2 = ("0002", "Michael", 40)
# emp3 = ("0003", "Matthew", 25)

#-------------------------------------------------------
# t = Table("Employees", column_mapping, ["ID"])
#
# # print(t.tuple_hasher(emp1))
# # print(t.tuple_hasher(emp1))
#
#
# # t.add_record(emp1)
# # t.add_record(emp2)
# # t.add_record(emp3)
# #
# # for x in t._select():
# #     print(x)
# # #
# # t.remove_record(emp1)
# #
# # for x in t._select():
# #     print(x)
#
#
#
# where = [{"symbol": "<", "column": "Age", "condition": 60}]
#
# # for x in t._select(where):
# #     print(x)
#
#
# t.update_record(["Age"], [20], where)
#
# #
# # where = [{"symbol": "==", "column": "Age", "condition": 30}]
# #
# # for x in t._select(where):
# #     print(x)
# #
# for x in t._select():
#     print(x)
#
# print(t.size)

# s = Table.create_from_join(6, 7)
#-------------------------------------------------------
#
# where = [{"symbol": "<", "column": "Age", "condition": 40}] #, {"symbol": "==", "column": "Name", "condition": "Brandon"}]
#
# left = Table("Employees1", column_mapping, ["ID"])
# right = Table("Employees2", column_mapping, ["ID"])
#
# # left.add_record(emp3)
# # right.add_record(emp3)
#
# for x in left._select_filter():
#     print(x)
#
# for x in right._select_filter():
#     print(x)
#
# print("-------------")
#
# # for x in left._select_filter(where):
# #     print(x)
#
# # for x in left._select_sort(["Age", "ID"], "asc"):
# #     print(x)
#
# # for x in left._select_filter(where, ["ID"], "desc"):
# #     print(x)
#
# # left.update_record(["Age"], [25], where)
#
# aggregates = [("Name", "count"), ("Age", "max")]
#
# # left.select(columns, aggregates, ["ID"])
# left.select(columns, aggregates, ["ID"])
#
# #
# # for x in left._select_filter():
# #     print(x)
#
# # joined = Table.create_from_join(left, right, "ID", "ID")
# #
# # for x in joined._select():
# #     print(x)
#
# # Tables[table_name].add_record(sfewfwef)
#
#
# # {table_name: }
#
#
# # tables[table_name].add_record(record)
#
