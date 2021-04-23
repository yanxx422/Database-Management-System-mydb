from table import * 
from column import *



def deserialized(obj):
    data = Interface.json.loads(obj)
    
    objt = Database(data['name'])
    
    
    for table_name, table_object in data['tables']:
        objt.add_table(table_name, Table.deseriazlied(table_object))
    
    # Return database object 
    return objt


class Database(Interface):
    
    def __init__ (self,name):
        self.__name = name
        self.__table_names = []
        self.__table_objects = {}
    
    
    
    def create_table(self, table_name, **options):
        
        if table_name in self.__table_objects:
            raise Exception ('This table exists.')
        
        self.__table_names.append(table_name)
        
        self.__table_objects[table_name] = Table (**options)
    
    
    
    def drop_table(self,table_name):
        if table_name not in self.__table_names:
            raise Exception('Table not exist')
        
        
        self.__table_names.remove(table_name)
        
        self. __table_objects.pop(table_name, True)
    
    
    def get_table_name(self, index = None):
        
        length = len(self.__table_names)
        
        if isinstance(index, int) and -index<length>index:
            return self.__table_names[index]
        
        return self.__table_names
    
    def get_table_objects(self,name):
        
        #Return None if table doesn't exist
        return self.__table_objects.get(name, None)
    
    def get_name(self):
        
        return self.__name
    
    def add_table(self,table_name,table):
        
        if table_name not in self.__table_objects:
            
            self.__table_names.append(table_name)
            
            self.__table_objects[table_name] = table 
    
    
    def serialized(self):
        
        data = {'name': self.__name, 'tables':[]}
        
        for table_name, table_data in self.__table_objects.items():
            
            data['tables'].append([table_name. table_data.serialized()])
            
        return Interface.json.dumps(data) 
    
    


import os 
import base64


def decode_db(content):
    content = base64.decodebytes(content)
    return content.decode()[::-1]



def encode_db(content):
    content = content[::-1].encode()
    return base64.encodebytes(content)

class API:
    
    def __init__(self,db_name = None, path = 'db.data'):
        
        self.__database_objects = {}
        
        self.__databae_names = []
        
        self.__current_db = None
        
        self.path = path 
        
        self.__load_databases()
        
        
    
    def create_database(self, database_name):
        
        if database_name in self.__database_objects:
            raise Exception('Database exists.')
        
        self.__database_names.append(database_name)
        
        self. __database_objects[database_name] = Database(database_name)
    
    
    def drop_database(self,database_name):
        
        if database_name not in self.__database_objects:
            
            self.__database_names.remove(database_name)
            
            self.__database_objects.pop(database_name, True)
        
    
    
    def select_database(self, database_name):
        
        if database_name not in self.__database_objects:
            
            raise Exception(' Database Not Exist.')
        
        self.__current_db = slef.__database_objects.values[database_name]
    
    
    
    def serialized(self):
        return Interface.json.dumps([
            database.serialized() for database in self.__database_objects.values()
        ])
    
    
    
    def save_database(self):
        with open(self.path, 'wb') as f :
            content = encode_db(self.serialized())
            
            f.write(content) 
    
    
    def deseriazlied(self,content):
        data= Interface.json.loads(content)
        
        for obj in data:
            database = Database.deserialized(obj)
            database_name = database.get_name()
            
            self.__database_names.append(database_name)
            
            self.__database_objects[database_name] = database 
            
    
    
    def load_database(self):
        
        if not os.path.exists(self.path):
            return 
        
        with open(self.path, 'rb') as f:
            
            content = f.read()
        
        if content:
            
            self.deserialized(decode_db(content))
    
    
    def commit_change(self):
        
        self.save_database()
    
    
    def roll_back(self):
        
        self.load_database()
        
            
    
    
    
       