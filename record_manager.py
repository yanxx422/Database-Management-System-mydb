from buffer_manager import *
from enum import Enum
import os 
import csv 
import sys

# Maximum size is 4096 bytes
BLOCK_MAX_SIZE = 4096
path = 'C:/Users/xiuyu/Dropbox/academi8c/mydb/data/'
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
            
      
        

      
    
    def add_row_data(self, table_name,column_names, column_values):
                
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
            
        
        table_file = path + table_name +'.csv'
        starting_block_id = os.path.getsize(table_file)//BLOCK_MAX_SIZE
        
        
        block = buffer (table_name + '.csv', starting_block_id)
        
    
        print (block)
        
        if getsizeof(block) + getsizeof(values) > BLOCK_MAX_SIZE:
            starting_block_id += 1 
            block = buffer(table_name + '.csv', starting_block_id)
        
        #print(starting_block_id)
        
        block_list = block.split('\n')
        # convert list to str
        record = ','.join([str(i) for i in column_values]) 
        

        block_list.append(record)  
        
        
        block_offset = block_list.index(record)  # block offset of insert position
        # convert to str in the block
        block_str = '\n'.join(block_list)  
        buffer_save (table_name + '.csv', starting_block_id,block_str)
        buffer_close()
        
        #Updating incides here....
        
        print("Succesfully inserted a row.")

            
        self.__rows += 1 
     

class Interpreter:
    
    def __init__(self):
        self.__table_names = []
        self.__table_objects = {}   
        
 
        

    
    def create_table(self,table_name, column_names, column_types, primary_key_column):
        if table_name in self.__table_objects:
            raise Exception ('This table exists.')
        
        self.__table_names.append(table_name)
    
        # TO DO: Clear buffer maybe ?
        
        
        table = Table()
        table.initialize_columns(column_names, column_types," ".join(primary_key_column))        
        self.__table_objects[table_name] = table
        
        file_path  = path + table_name + '.csv'
        with open(file_path, 'w') as f:
            print('csv created!')
        pass
    
 
    
    def insert (self,table_name,columns,values):
        
        if table_name not in self.__table_names:
            raise Exception ('You tried to insert to a table that does not exist. Create it first.')
        
        
        self.__table_objects[table_name].add_row_data(table_name,columns, values)
        
        
        
        
    
    def print_this_table(self, table_name):
        pass
        #self.__table_objects[table_name].display_this_table()
    
    


if __name__ == '__main__':
    

    table_name = "persons"
    primary_key_column = ['ID']
    column_types = ['int', 'varchar', 'varchar']
    column_names = ['ID', 'lastname', 'City']
    
    engine = Interpreter()

    engine.create_table(table_name, column_names, column_types, primary_key_column)
    
    values = [1, "Colbert", "New York"]
    
    columns = ['ID', 'lastname', 'City']
    
    engine.insert(table_name,columns,values)
    
    values = [2, "Jing", "Beijing "]
    
    columns = ['ID', 'lastname', 'City']
    
    engine.insert(table_name,columns,values)
    
