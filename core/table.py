from column import *

from prettytable import PrettyTable

import ast 


class Table(Interface):
    
    def __init__(self):
        
        self.__column_names = []
        self.__column_objects = {}   
        self.__rows = 0
        

    
    def initialize_columns(self, options, primary_key_columns):
        
        
        for column_name, column_type in options.items():
     
            self.__column_names.append(column_name)
            
            for primary_key_column in primary_key_columns:
                if column_name == primary_key_column:
                    self.__column_objects[column_name] = Column(column_type,"PRIMARY")
                else:
                    self.__column_objects[column_name] = Column(column_type)
             

        
    
    def add_columns(self, options):
        for column_name, column_value in options.items():
            #print(column_name)
            if column_name not in self.__column_names:
                raise Exception('This column doesnt exist.')                   
            self.get_column(column_name).add_data(column_value)
                    
        self.__rows += 1  
    
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

                
if __name__ == '__main__':
    
    name = Table()
    
    
    options = {'ID': 'int', 'lastname': 'varchar', 'City': 'varchar'}
    primary_key_columns = ['ID']
    name.initialize_columns(options.copy(), primary_key_columns)
 
    
    table_name = "persons"
    values = [1, "Colbert", "New York"]
    
    columns = ['ID', 'lastname', 'City']
    options = dict(zip(columns, values))
    name.add_columns(options)
    
    
    
    
    
    table_name = "persons"
    vals = [3, "jing", "Beijing"]
    
    cols = ['ID', 'lastname', 'City']
    optionss = dict(zip(cols, vals))
    name.add_columns(optionss)
    
    name.print_all_columns()
  
        

                
                
        
        
        
        
        
