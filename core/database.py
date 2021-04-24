from table import *

def deserialized(obj):
 
    data = Interface.json.loads(obj)

 
    obj_tmp = Database("mydb")

    for table_name, table_obj in data['tables']:
        obj_tmp.add_table(table_name, Table.deserialized(table_obj))

    
    return obj_tmp

class Database(Interface):
    
    def __init__ (self,name):
        self.__table_names = []
        self.__table_objects = {}

    def create_index(self,indexed_column,index_name,table_name):
        pass 
    
    
    def drop_index(index_name):
        pass 
    
    def update_where(table_name,columns_to_be_updated,values_to_be_updated,where):
        pass
    def update(table_name,columns_to_be_updated,values_to_be_updated):
        pass 
    
    
    def create_index(self,indexed_column,index_name,table_name):
        pass 
    
    def delete(self,table_name, where):
        pass 
    
    def insert (self,table_name,columns,values):
        option = dict(zip(columns, values))
        
        if table_name not in self.__table_names:
            raise Exception ('You tried to insert to a table that does not exist. Create it first.')
        
        
        self.__table_objects[table_name].add_columns(option)
        
    
    
    def create_table(self,table_name, column_names, column_types, primary_key_column):
        
        if table_name in self.__table_objects:
            raise Exception ('This table exists.')
        
        self.__table_names.append(table_name)
        
        
        options = dict(zip(column_names, column_types))
        table = Table()
        table.initialize_columns(options,primary_key_column)        
        self.__table_objects[table_name] = table
        

      
    
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
    

    def get_name(self):
        
        return self.__name
    


    
    
    def print_table(self,table_name):
        self.__table_objects[table_name].print_all_columns()
        
      

    
    def serialized(self):
        
        data = {'tables': []}
        
        #print(data)
        
        for table_name, table_data in self.__table_objects.items():

            pass
            #data['tables'].append([table_name,table_data.serialized()])
        
        return Interface.json.dumps(data) 
    
    



import os 
import base64


def decode_db(content):
    content = base64.decodebytes(content)
    return content.decode()[::-1]



def encode_db(content):
    content = content[::-1].encode()
    return base64.encodebytes(content)




class Engine:
    
    def __init__(self, path = 'db.data'):
        
        self.path = path 
        self.database_objs = []
        
        
        self.load_database()
        
        
        self.db = Database("mydb")
    
    def create_table(self,table_name, column_names, column_types, primary_key_column):
        
        self.db.create_table(table_name, column_names, column_types, primary_key_column)
        self.commit_change()
    
    
    def create_index(self,indexed_column,index_name,table_name):
        self.db.create_index(indexed_column,index_name,table_name)
    
    def insert(self, table_name,columns,values):
        self.db.insert(table_name,columns,values)
        pass
    
    def drop_table(self,table_name):
        self.db.drop_table(table_name)
    
    def drop_index(self,index_name):
        self.db.drop_index(index_name)
    
    def update(self,table_name,columns_to_be_updated,values_to_be_updated):
        self.db.update(table_name,columns_to_be_updated,values_to_be_updated)
    
    def update_where(self,table_name,columns_to_be_updated,values_to_be_updated,where):
        self.db.update(table_name,columns_to_be_updated,values_to_be_updated,where)
    
    
    def delete(self,table_name, where):
        self.db.delete(table_name, where)
        
    
    def print_table(self,table_name):
        self.db.print_table(table_name)
    
    
    def serialized(self):
        return Interface.json.dumps(
            self.db.serialized()
        )
    
    
    
    def save_database(self):
        with open(self.path, 'wb') as f :
            content = encode_db(self.serialized())
           
           
            f.write(content) 
    
    
    def deserialized(self,content):
        data= Interface.json.loads(content)
        
        self.db = deserialized(data)
            
    
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





if __name__ == '__main__':
    

    table_name = "persons"
    primary_key_column = ['ID']
    column_types = ['int', 'varchar', 'varchar']
    column_names = ['ID', 'lastname', 'City']
    
    engine = Engine()

    engine.create_table(table_name, column_names, column_types, primary_key_column)
    
    #engine.commit_change()

   
    values = [1, "Colbert", "New York"]
    
    columns = ['ID', 'lastname', 'City']
    
    engine.insert(table_name,columns,values)
    
    engine.print_table(table_name)
    
    #engine.drop_table(table_name)
    
            
    
    
    
       
    
            
    
    
    
       
    
    
       
