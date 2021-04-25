

'''

CLASSES:
Table
Column
Index





'''


class Table:

    def __init__(self, table_name, column_mapping):
        self.table_name = table_name
        self.column_mapping = column_mapping


    @classmethod
    def create_from_join(cls, left_table, right_table):
        table_name = None
        column_mapping = None

        joinedTable = cls(table_name, column_mapping)

        return joinedTable



t = Table(5, 5)

s = Table.create_from_join(6, 7)


