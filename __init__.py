import re
from cmd import Cmd

def find_2nd(string, substring):
    return string.find(substring, string.find(substring) + 1)

def auto_type(value: str):
    
    
    if value[0] == "'" and value[-1] == "'":
        value = value[1:-1]
    elif re.match(r'^-?[0-9]+\.+[0-9]+$', value):
        value = float(value)
    elif re.match(r'^-?[0-9]+', value):
        value = int(value)
    else:
        pass
        
    return value




class SQLParser:
    def __init__(self):
        self = self 


   #create table persons( ID int, lastname VARCHAR, City VARCHAR, PRIMARY KEY (ID));
    def create(self,arg: str):
        print('Create!')
        
        arg =arg.strip()
        
        if arg[-1] == '(': 
            while 1:
                line = input().rstrip()
                arg = arg + line
                if line[-1] != ',' and line[-1] != ')':
                    break              
        
        arg = arg.rstrip(';').strip()
        arg = re.sub(' +', ' ', arg)  
  
        if arg[:5] == 'table':
            arg = arg[5:]
            arg = arg.lstrip()
            table_name = arg[:arg.find('(')].strip()  
            if table_name == '':
                raise Exception("Table name is empty")
           
            
            arg = arg[arg.find('('):]
            arg = arg.lstrip('(').strip()
            if arg[-1] == ')':
                arg = arg[:-1]
            arg = arg.strip()
            if arg == '':
                raise Exception("No table specification found.")
            column_specifications  = arg.split(',')
            column_specifications  = list(map(str.strip, column_specifications ))
            if column_specifications  == []:
                raise Exception("No table columns found.")
            primary_key_column = []
            if column_specifications [-1].startswith('primary key') or column_specifications [-1].startswith('PRIMARY KEY'):
                
                lbracket_position = column_specifications [-1].find('(')
                rbracket_columns_position = column_specifications [-1].find(')')
                
                primary_key_column = column_specifications [-1][lbracket_position + 1: rbracket_columns_position].split(',')   
                
                #Exclude that primary_key_column from the string
                column_specifications = column_specifications[:-1]
            
            
            column_types = []
            column_names = []
            
            #column_specifications = ['id int', 'lastname varchar', 'firstname varchar', 'address varchar', 'city varchar \n);']
            for column_specification in column_specifications:
                item = column_specification.split(' ')
                
                
                column_names.append(item[0])
                column_types.append(item[1].lower())
            
            #Example column_names: ['ID', 'lastname', 'City']
            print(column_names)
            # Types are all in lower cases
            #Example column_types: ['int', 'varchar', 'varchar'
            print(column_types)
            #Example primary_key_column ['ID']
            print(primary_key_column)
            print(table_name)
            
            # Pass column_names, column_types,primary_key_column, table_name to somewhere 
            
            # TO DO .....
            
        
        #CREATE INDEX index_name ON tableName (tableColumn);
        elif arg[:5].lower() == 'index':
            
            arg =arg[5:] 
            
            arg = re.sub(' +', ' ', arg)
            
            on_position = arg.lower().find('on')
            
            if on_position == -1:
                raise Exception(f"'on' is missing when creating index.")
            index_name = arg[:on_position].strip()
            
        
            location_lbracket = arg.find('(')
            if location_lbracket == -1:
                raise Exception(f"Indexed attribute format is wrong.")
            table_name = arg[on_position + len('on'): location_lbracket].strip()   
        
    
        
            location_rbracket = arg.find(')')
            if location_rbracket == -1:
                raise Exception(f"Indexed attribute format is wrong.")
            indexed_column = arg[location_lbracket + 1: location_rbracket].strip()
        
            if ',' in indexed_column:
                raise Exception('Only single attribute index is supported.')
        
            print(indexed_column)
            print(index_name)
            print(table_name)
            # TO DO 
            # Pass indexed_column, index_name and table_name to somewhere 
        
        else:
            raise Exception("Create Syntax is not valid.")


    #DROP TABLE Shippers;
    #DROP INDEX index_name;
    
    def drop(self,arg: str):
        print("drop!")
        arg = arg.rstrip(';')
        arg =  " ".join(arg.split())
        lower_arg = arg.lower()
            
        if lower_arg[:5] == 'table':
            table_name = arg[5:].strip()
            print(table_name)
            
            #TO DO: pass table_name to somewhere!!
            
        elif lower_arg[:5] == 'index':
            index_name = arg[5:].strip()
            print(index_name)
            
            #TO DO: pass index_name to somewhere!!
            
        else:
            raise Exception ("Drop statement syntax is not valid. ")


    
    #INSERT INTO table_name (column1, column2, column3, ...) VALUES (value1, value2, value3, ...); 
    #INSERT INTO Customers (    Address, City, PostalCode, Country)     VALUES ('Skagen 21', 'Stavanger', 4006, 'Norway');
    def insert(self,arg: str):
        print('insert!') 
        arg = arg.rstrip(';')
        # Get rid of extra space
        arg = re.sub(' +', ' ', arg).strip()
        
        into_positoin = arg.lower().find('into')
        if into_positoin == -1:
            raise Exception('INSERT INTO syntax is invalid.')
        values_position = arg.lower().find('values')
        if values_position == -1:
            raise Exception("INSERT INTO syntax is invalid, values is missing.")
        
        lbracket_values_position = find_2nd(arg,'(')
        rbracket_values_position = find_2nd(arg,')')
        values = arg[lbracket_values_position + 1: rbracket_values_position].split(',')
        values = list(map(str.strip, values))
        values = list(map(auto_type, values))
        

        table_info = arg[into_positoin + 4:values_position].strip()
        lbracket_columns_position = table_info.find('(')
        rbracket_columns_position = table_info.find(')')
        
        columns = table_info[lbracket_columns_position + 1: rbracket_columns_position].split(',')     
        columns = list(map(str.strip, columns))   
  
        
        table_name = arg[into_positoin+4:lbracket_columns_position+4].strip(' ')
 
             
        print(table_name)        
        print(values)
        print(columns)
        
        
        
    #DELETE FROM Customers WHERE CustomerName = Alfreds Futterkiste AND ID = 4;
    #DELETE FROM Customers
    def delete(self,arg: str):
        arg = arg.rstrip(';')
        arg = re.sub(' +', ' ', arg).strip()
        
        arg_lower = arg.lower()
        from_position = arg_lower.find('from')
        if from_position == -1:
            raise Exception("'from' is missing.")
    
        where_position = arg_lower.find('where')
        if where_position == -1:
            table_name = arg[from_position + 4:].strip()
            print(table_name)
            # No where clase, pass table_name to somewhere !
            # TO DO ... 
            
        else:
            table_name = arg[from_position + 4: where_position].strip()
            if 'AND' in arg:
                
                conditions = arg[where_position + len('where'):].strip().split('AND')
            else:
                conditions = arg[where_position + len('where'):].strip().split('and')
    
            conditions = list(map(str.strip, conditions))
            where = []
            for condition in conditions:
                operators = ['=', '<>', '<', '>', '<=', '>=']
                no_operator = True
                for operator in operators:
                    if operator in condition:
                        op = operator
                        location = condition.find(op)
                        l_op = condition[:location].strip()
                        r_op = condition[location + 1:].strip()
                        no_operator = False
                        break
                if no_operator:
                    raise Exception(f"no operator found in {condition}")
                r_op = auto_type(r_op)
                where.append({'symbol': operator, 'column_name': l_op, 'condition': r_op})
            print(where)
            print(table_name)
            # where clase, pass table_name and where to somewhere !
            # TO DO ...                 

    def filter_space(self, obj):
        ret = []
        for x in obj:
            if x.strip() == '' or x.strip() == 'AND' or x.strip() == 'and':
                continue
            ret.append(x)

        return ret



# select id,name  from students where id > 3 and  name = Jerry


    def select(self,arg:str):
        arg = arg.rstrip(';')
        if 'where' in arg:
            arg = arg.split('where')
        else:
            arg = arg.split('WHERE')
            
        # base_statement =  ['id,', 'name', 'from', 'students']
        base_statement = self.filter_space(arg[0].split(" "))
        
    
        pattern = r'(.*) (FROM|from) (.*)'
        comp = re.compile(pattern)
        ret = comp.findall(" ".join(base_statement))
        # ret =  [('id, name', 'from', 'students')]
    
        if ret and len(ret[0]) == 3:
    
            columns = ret[0][0]        
            where = []
                
            if 'JOIN'in ret[0][2] or 'join'in ret[0][2]:
                # For now only deal with one join condition, and not including "and" in join condition
                pattern = r'(.*) (JOIN|join) (.*) (ON|on) (.*)(\.)(.*)(=|>|<|>=|<|<=|<>) (.*)(\.)(.*)'
                comp  = re.compile(pattern)
                new_ret = comp.findall((ret)[0][2])
                table_name = new_ret[0][0]

                new_columns = []
                if columns != '*':
                    columns = [column.strip() for column in columns.split(",")]
                    
                    for column in columns:
                        new_columns.append(str(column).split("."))
                        
                    
                    if len(arg) == 2:
                        conditions = self.filter_space(arg[1].split(" "))     
                        if conditions:
                            for i in range(0, len(conditions),3):
                                table_column =  conditions[i]
                                pattern = r'(.*)(\.)(.*)'
                                comp  = re.compile(pattern)
                                new_ret = comp.findall(table_column)

                                where.append({'symbol': conditions[i+1], 'table': new_ret[0][0],'column':new_ret[0][2], 'condition': auto_type(conditions[i+2])})   
                                
                # call function
                print(new_columns)
                print(table_name)
                print(where)            

                
            else:  
                table_name = ret[0][2]
                
                if columns != '*':
                    columns = [column.strip() for column in columns.split(",")]
                    
                    if len(arg) == 2:
                        conditions = self.filter_space(arg[1].split(" "))     
                        # conditions = ['id', '=', '6']
                        if conditions:
                            for i in range(0, len(conditions),3):
                                where.append({'symbol': conditions[i+1], 'column': conditions[i], 'condition': auto_type(conditions[i+2])})
    
                print(columns)
                print(table_name)
                print(where)            
              
                
                
                # call function 


    
    # UPDATE Customers SET ContactName='Juan';
    # UPDATE Customers SET ContactName='Alfred Schmidt', City='Frankfurt' WHERE CustomerID = 1;
    def update(self,arg: str):
        
        arg = arg.rstrip(';')
        arg = re.sub(' +', ' ', arg).strip()
        
        columns_to_be_updated = []
        values_to_be_updated = []
        where = []
        
        location_set = arg.lower().find('set')
        if location_set == -1:
            raise Exception("'set' is missing.")
    
        location_where = arg.lower().find('where')
        
        if location_where == -1:
    
            # Customers SET ContactName='Juan';
            pattern = r'(.*) (SET|set) (.*)'
            comp = re.compile(pattern)
            ret = comp.findall("".join(arg))
          
            table_name = ret[0][0]
            
            column_specifications = ret[0][2]
            
            for column_specification in column_specifications.split(','):
                
                
                pattern = r'(.*) (=) (.*)'
                comp  = re.compile(pattern)
                new_ret = comp.findall(column_specification)
                columns_to_be_updated.append(new_ret[0][0])
                values_to_be_updated.append(auto_type(new_ret[0][2]))
            
            print(table_name)
            print(columns_to_be_updated)
            print(values_to_be_updated)
            
            
            #TO DO: PASS THESE PARAMETERS TO SOMEWHERE 
                
        else:
            table_name = arg[: location_set].strip()
            
            if 'AND' in arg:
                    
                conditions = arg[location_where + len('where'):].split('AND')
                
            else:
                conditions = arg[location_where + len('where'):].split('and')
             
                
            
            conditions = list(map(str.strip, conditions))
            #print(conditions)
            
            for condition in conditions:
    
                pattern = r'(.*) (=) (.*)'
                comp  = re.compile(pattern)
                new_ret = comp.findall(condition)
                #print(new_ret)
                where.append({'column_name':new_ret[0][0], 'value':auto_type(new_ret[0][2])})
    
            arg = arg[:location_where]
            pattern = r'(.*) (SET|set) (.*) '
            comp = re.compile(pattern)
            ret = comp.findall("".join(arg))
     
            column_specifications = ret[0][2].split(',')
            
            for column_specification in column_specifications:
                
                pattern = r'(.*) (=) (.*)'
                comp  = re.compile(pattern)
                new_ret = comp.findall(column_specification)
              
                columns_to_be_updated.append(new_ret[0][0])
                values_to_be_updated.append(new_ret[0][2])
            
            print(table_name)
            print(where)
            print(columns_to_be_updated)
            print(values_to_be_updated)
            
            #TO DO: PASS THESE PARAMETERS TO SOMEWHERE 
        
        
        

    def show(self,arg: str):
        print('show!')

    def exit(self,arg: str):
        print('exit!')






class Runner(Cmd):
    def __init__(self):
        Cmd.__init__(self)

    def do_create(self,arg:str):
        try:
            SQLParser().create(arg)
        except Exception as e:
            print('Creating Failed.', e)
            
    def do_CREATE(self,arg:str):
        try:
            SQLParser().create(arg)
        except Exception as e:
            print('Creating Failed.', e)    

    def do_drop(self,arg:str):
        try:
            SQLParser().drop(arg)
        except Exception as e:
            print('Dropping Failed.', e)
            
    def do_DROP(self,arg:str):
        try:
            SQLParser().drop(arg)
        except Exception as e:
            print('Dropping Failed.', e)
    

    def do_select(self,arg:str):
        try:
            SQLParser().select(arg)
        except Exception as e:
            print('Selecting Failed.', e)
            
    def do_SELECT(self,arg:str):
        try:
            SQLParser().select(arg)
        except Exception as e:
            print('Selecting Failed.', e)    

    def do_insert(self,arg:str):
        try:
            SQLParser().insert(arg)
        except Exception as e:
            print('Inserting Failed.', e)
            
    def do_INSERT(self,arg:str):
        try:
            SQLParser().insert(arg)
        except Exception as e:
            print('Inserting Failed.', e)


    def do_delete(self,arg:str):
        try:
            SQLParser().delete(arg)
        except Exception as e:
            print('Deleting Failed.', e)
            
    def do_DELETE(self,arg:str):
        try:
            SQLParser().delete(arg)
        except Exception as e:
            print('Deleting Failed.', e)
            
    def do_update(self,arg:str):
        try:
            SQLParser().update(arg)
        except Exception as e:
            print('Updating Failed.', e)
            
    def do_UPDATE(self,arg:str):
        try:
            SQLParser().update(arg)
        except Exception as e:
            print('Updating Failed.', e)     
            
    def do_show(self,arg:str):
        try:
            SQLParser().show(arg)
        except Exception as e:
            print('Showing Failed.: ', e)

    def do_exit(self,arg:str):
        print("See you.")
        return True

    def default(self, line: str):
        print(f"Unknown command: {line.split(' ')[0]}")

if __name__ == "__main__":
    Runner().cmdloop()
