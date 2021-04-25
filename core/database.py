from beautifultable import BeautifulTable
from enum import Enum
from bplustree import BPlusTree, UUIDSerializer
import pickle
import hashlib
import uuid
import os
from enum import Enum
path_variable = "/Users/..../mydb/"

class ColumnType(Enum):
    INT = int = 'int'
    VARCHAR = varchar = 'str'
    FLOAT= float = 'float'
    
TYPE_MAP = {
    'int':int,
    'float':float,
    'str': str,
    'INT':int,
    'FLOAT': float,
    'VARCHAR': str,
    'varchar': str
}

        
class Table:
    
    def __init__(self):
        self.primary_key = ''
        self.__column_names = []
        self.__column_types= []
        self.__rows = 0
        #self.data = BPlusTree('/Users/haddadb1/mydb/' + table_name + ".db", serializer=UUIDSerializer(), key_size=32)
        self.data = BPlusTree('C:/Users/xiuyu/Dropbox/academi8c/mydb/data' + table_name + ".db", serializer=UUIDSerializer(), key_size =50)
        self.indices = []
        self.primary_key_column_values = []
    
  
    #primary_key_column = 'ID'
    #column_names = ['int', 'varchar', 'varchar']
    #column_names = ['ID', 'lastname', 'City']
    def initialize_columns(self, column_names, column_types, primary_key_column):
        
        for i in range(len(column_names)): 
            if not (column_types[i] in ColumnType.__members__):
                raise TypeError ('Data type is not valid')
        self.__column_types = column_types

        if len(set(column_names)) != len(column_names):
            raise Exception("Can't have duplicate column names.")
        else:
            self.__column_names = column_names
            
        if primary_key_column not in column_names:
            raise Exception("primary_key '%s' does not exist." % self.primary_key)
        else:
            self.primary_key = primary_key_column
        

      
    
    def add_row_data(self, column_names, column_values):
        data_key = uuid.uuid1()
        self.data.insert(data_key, pickle.dumps(column_values))
        
        for column_name in column_names:
            if column_name not in self.__column_names:
                raise Exception("Must add data to existing column.")


        for i in range(len(self.__column_names)):
            #Check if primary key column's values are unique
            if self.primary_key == self.__column_names[i]:
                self.primary_key_column_values.append(column_values[i])
                
            #Check if the record type is corresponding to the column type
            
            if not isinstance(column_values[i], TYPE_MAP[self.__column_types[i]]):
                raise TypeError('data type error, value must be %s' % self.__column_types[i])

    
        #Check if primary key contains repetitive elements      
        if len(self.primary_key_column_values) != len(set(self.primary_key_column_values)):
            raise Exception("Primary key volumn can't have repetitive elements.")
        
  
        # update possible indices
            
            
        for index in self.indices:
            index.index_record(column_values, data_key)
                
            
        self.__rows += 1 
        self.data.checkpoint()
        
        
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
        
    
    def display_this_table(self):
        table = BeautifulTable()
        table.rows.header = self.__column_names
        print(table)

        try: 
            for key, value in self.data.items():
                pass
                #print(pickle.loads(value))

        except: 
            pass
    
    def close(self):
        self.data.close()


        


        
        
        
        

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
        
        
        table = Table()
        table.initialize_columns(column_names, column_types," ".join(primary_key_column))        
        self.__table_objects[table_name] = table
        
    
    def insert (self,table_name,columns,values):
        
        if table_name not in self.__table_names:
            raise Exception ('You tried to insert to a table that does not exist. Create it first.')
        
        
        self.__table_objects[table_name].add_row_data(columns, values)
        
    
    def print_this_table(self, table_name):
        self.__table_objects[table_name].display_this_table()
    
    
    def print_all_records(self, table_name):
        self.__table_objects[table_name].display_all_records()
        
        
        
    def close_all_bplustrees(self):
        for table,table_object in self.__table_objects.items():
            self.__table_objects[table_name].close()
            
            


         
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
    
    values = [2, "Jing", "Beijing "]
    
    columns = ['ID', 'lastname', 'City']
    
    engine.insert(table_name,columns,values)
    engine.print_all_records(table_name)
    engine.print_this_table(table_name)
    
    engine.close_all_bplustrees()
    

        
        
    
       
    
            
    
    
    
       
    
    
       
