from column import *



def deserialized(data):

    json_data = Interface.json.loads(data)

    table_obj = Table()

    column_names = [column_name for column_name in json_data.keys()]


    for column_name in column_names:

        column_obj = Column.deserialized(json_data[column_name])

        table_obj.add_column(column_name, column_obj)

    return table_obj


class Table(Interface):
    
    def __init__(self, **options):
        
        self.__column_names = []
        self.__column_objects = {}
        self.__rows = 0
        
        for column_name. column_object in options.items():
            self.add_column(column_name, column_objects)
        
        
        # Add new column
        def add_column(self, column_name, column_object, value = None):
            
            if column_name in self.__column_names:
                raise Exception('This column exists.')
            
            if not isinstance(column_object, Column):
                raise TypeError ('type error, value must be %s' % Column)
                
            
            self.__column_names.append(column_name)
            
            self.__column_objects[column_name] = column_object
            
            # If there already exists other column names, syncronize the length of columns
            if len (self.__column_names) > 1:
                
                length = self.__rows
                
                column_object_lenth = column_object.length()
                
                if column_object_length != 0:
                    
                    if column_object_length == length:
                        return 
                    
                    else:
                        raise Exception ('Column data length is not valid.')
                
                for index in range (0,length):
                    
                    if value:
                        self.get_column(column_name).add_data(vlaue)
                    
                    else:
                        self.get_column(column_name).add_data(Non)
            
            else:
                self.__rows = column_object.length()
    
    # Get column object 
    def get_column(self, column_name):
        
        if column_name not in self.__column_names:
            raise Exception('%s column not exists' % column_name)
        
        return self.__column_objects[column_name]
    
    def serialized(self):
        data = {}
        for field in self.__column_names:
            data[field] = self.__column_objects[field].serialized()

        return Interface.json.dumps(data)

    
    
    # Get column data     
    def get_column_data(self, column_name, index = None):
        column = self.get_column(column_name)
        
        return column.get_data(index)
    
    def get_column_type(self,column_name):
        column = self.get_column(column_name)
        
        return column.get_type()
    
    
    def get_parameter_list(self, **options):
        
        parameter_list = []
        
        params = options
        
        for column_name in params.keys():
            if column_name not in self.__column_names:
                raise Exception('%s Column is not valid.' % field_name)
            
            else: 
                parameter_list.append(column_name)
        
        return paramter_list
    
    def insert (self, **data):
        
        if 'data' in data:
            data = data['data']
        
        param_list = self.get_parameter_list(**data)
        
        
        for column_name in self.__column_names:
            
            value = None 
            
            if column_name in param_list:
                
                value = data[column_name]
            
            else:
                
                try: 
                    self.__get_column(column_name).add_data(value)
                
                except Exception as e:
                    raise Exception(column_name, str(e))
            
        
        self.__rows += 1 
        
    

                
                
        
        
        
        
        