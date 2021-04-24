from prettytable import PrettyTable
from enum import Enum
from bplustree import BPlusTree, UUIDSerializer
import pickle
import hashlib
import uuid
path_variable = "/Users/..../mydb/"

class ColumnConstraints(Enum):
    PRIMARY = 'PRIMARY KEY'
    UNIQUE = 'UNIQUE'
    NOT_NULL = 'NOT NULL'
    NULL = 'NULL'



class ColumnType(Enum):
    INT = int = 'int'
    VARCHAR = varchar = 'str'
    FLOAT= float = 'float'
    BOOL = bool = 'bool'


TYPE_MAP = {
    'int':int,
    'float':float,
    'str': str,
    'INT':int,
    'FLOAT': float,
    'VARCHAR': str,
    'varchar': str,
    'BOOL':bool,
    'bool':bool
    
}

class Column():

    def __init__(self, data_type, constraints = "NULL", default = None):
        self.__type = data_type
        self.__constraints = constraints
        self.__default = default
        self.__values = []
        self.__rows = 0

        if not isinstance(self.__constraints, list):
            self.__constraints = [self.__constraints]

        if not (self.__type in ColumnType.__members__):
            raise TypeError ('Data type is not valid')

        for constraint in self.__constraints:
            if not (constraint in ColumnConstraints.__members__):
                raise TypeError('Constraint type is not valid.')

        if self.__default is not None and "UNIQUE" in self.__constraints:
            raise Exception ('Unique constraint is not allowed to set default null value.')


    def check_index(self,index):
        if not isinstance(index,int) or not -index < self.__rows > index:
            raise Exception ('Index not valid, not this element.')
        return True


    def check_type(self,value):
        if value is not None and not isinstance(value, TYPE_MAP[self.__type]):
            raise TypeError('data type error, value must be %s' % self.__type)

    def check_constraints(self,value):
        # Data Entry must be unique for primary and unique constraints
        if "PRIMARY" in self.__constraints or "Unique" in self.__constraints:
            if value in self.__values:
                raise Exception('value %s exists' % value)

        if ("PRIMARY" in self.__constraints or "NOT NULL" in self.__constraints) and value is None:
            raise Exception('Column Not Null')
        return value


    def length(self):
        return self.__rows

    def get_type(self):
        return self.__type

    def get_constraints(self):
        return self.__constraints

    def get_data(self,index = None):

        #If index is an int, return the specific data
        if index is not None and self.check_index(index):
            return self.__values[index]

        #Otherwise, return all the deta of the column
        return self.__values

    def add_data(self, value):
        #If inserting empty data, then set it to defalt value
        if value is None:
            value = self.__default

        value = self.check_constraints(value)

        self.check_type(value)

        self.__values.append(value)

        self.__rows += 1
        

    def modify_data(self, index, value):

        self.check_index(index)

        value = self.check_constraints(value)

        self.check_type(value)

        self.__values[index] = value
        

    def delete_data(self, index):

        self.check_index(index)

        self.__values.pop(index)

        self.__rows -= 1



class Table:
    
    def __init__(self):
        
        self.__column_names = []
        self.__column_objects = {}   
        self.__rows = 0
        #self.data = BPlusTree('/Users/haddadb1/mydb/' + table_name + ".db", serializer=UUIDSerializer(), key_size=32)
        self.data = BPlusTree('C:/Users/xiuyu/Dropbox/academi8c/' + table_name + ".db", serializer=UUIDSerializer(), key_size=32)
        self.indices = []
        

    
    def initialize_columns(self, options, primary_key_columns):
        
        
        for column_name, column_type in options.items():
     
            self.__column_names.append(column_name)
            
            for primary_key_column in primary_key_columns:
                if column_name == primary_key_column:
                    self.__column_objects[column_name] = Column(column_type,"PRIMARY")
                else:
                    self.__column_objects[column_name] = Column(column_type)
             

        
    
    def add_row_data(self, options):
        for column_name, column_value in options.items():
            #print(column_name)
            if column_name not in self.__column_names:
                raise Exception('This column doesnt exist.')  
                             
            self.get_column(column_name).add_data(column_value)
            
            data_key = uuid.uuid1()
            self.data.insert(data_key, pickle.dumps(column_value))
            # update possible indices
            for index in self.indices:
                index.index_record(column_value, data_key)


                   
        self.__rows += 1 
        
    def add_index(self, attribute_name, data):

        # get index of attribute from columns list
        try:
            record_pos = self.columns.index(attribute_name)
            self.indices.append(Index(self.table_name, attribute_name, record_pos, self.data))
        except:
            print("ERROR: Attribute does not exist.")
    
    def get_column(self, column_name):
        
        if column_name not in self.__column_names:
            raise Exception('%s column not exists' % column_name)
        
        return self.__column_objects[column_name]
    
    def print_all_columns(self):
        pt = PrettyTable()
        for column_name in self.__column_names:
            
                
            
        
            ret = self.__column_objects[column_name].get_data()
            
            
            if ret:
                pt.add_column(column_name, ret)
                
        print(pt)
        
        
        
        

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
class Interpreter:
    
    def __init__(self):
        
        self.__table_names = []
        self.__table_objects = {}   
    
    def create_table(self,table_name, column_names, column_types, primary_key_column):
        if table_name in self.__table_objects:
            raise Exception ('This table exists.')
        
        self.__table_names.append(table_name)
        
        
        options = dict(zip(column_names, column_types))
        table = Table()
        table.initialize_columns(options,primary_key_column)        
        self.__table_objects[table_name] = table
        
    
    def insert (self,table_name,columns,values):
        option = dict(zip(columns, values))
        
        if table_name not in self.__table_names:
            raise Exception ('You tried to insert to a table that does not exist. Create it first.')
        
        
        self.__table_objects[table_name].add_row_data(option)
        
    
    def print_this_table(self, table_name):
        self.__table_objects[table_name].print_all_columns()
        
        
        
    def close_all_bplustrees(self):
        for table,table_object in self.__table_objects.items():
            self.__table_objects[table_name].data.close()
            
            


         
if __name__ == '__main__':
    


    table_name = "persons"
    primary_key_column = ['ID']
    column_types = ['int', 'varchar', 'varchar']
    column_names = ['ID', 'lastname', 'City']
    
    engine = Interpreter()

    engine.create_table(table_name, column_names, column_types, primary_key_column)
    
    #engine.commit_change()

   
    values = [1, "Colbert", "New York"]
    
    columns = ['ID', 'lastname', 'City']
    
    engine.insert(table_name,columns,values)
    
    values = [2, "Jing", "Bei jing "]
    
    columns = ['ID', 'lastname', 'City']
    
    engine.insert(table_name,columns,values)
    
    engine.print_this_table(table_name)
    
    engine.close_all_bplustrees()
        
        
    
       
    
            
    
    
    
       
    
    
       
