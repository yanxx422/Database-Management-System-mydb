import re
from cmd import Cmd
import json
from dataManagement import Table
import os.path
#tables maps table name to table objects
tables = {}

#metadata maps table name to {column names, column types}
metadata = {}

    

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


def strip_list_noempty(mylist):
    newlist = (item.strip() if hasattr(item, 'strip') else item for item in mylist)
    return [item for item in newlist if item != '']

class SQLParser:
    def __init__(self):
        self = self 

   #create table persons( ID int, lastname varchar, City varchar, PRIMARY KEY (ID));
    def create(self,arg: str):
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
            

            
            for key, value in tables.items():
                if key == table_name:
                    raise Exception ("Table alreay exists.")
            
            
            # Update metadata
            metadata[table_name] = [table_name,dict(zip(column_names,column_types)),primary_key_column]
            
            # Pass column_names, column_types,primary_key_column, table_name to somewhere 
            myTable = Table(table_name, dict(zip(column_names,column_types)), primary_key_column)
            
            # Add table object to tables
            tables[table_name] = myTable
            
            
            
        
        #CREATE INDEX index_name ON tableName (tableColumn);
        elif arg[:5].lower() == 'index':
            
            arg =arg[5:] 
            
            print(arg)
            arg = re.sub(' +', ' ', arg)
            
            on_position = arg.lower().find('on')
            
            if on_position == -1:
                raise Exception(f"'on' is missing when creating index.")
            
            
            table_name = arg[3:][on_position:].strip()
            
            location_lbracket = arg.find('(')
            if location_lbracket == -1:
                raise Exception(f"Indexed attribute format is wrong.")
            table_name = arg[3:][on_position: location_lbracket-3].strip() 

            location_rbracket = arg.find(')')
            if location_rbracket == -1:
                raise Exception(f"Indexed attribute format is wrong.")
            indexed_column = arg[location_lbracket + 1: location_rbracket].strip()
            
            if ',' in indexed_column:
                raise Exception('Only single attribute index is supported.')
                
            
            tables[table_name].create_index(indexed_column)
            
            
    
        
        else:
            raise Exception("Create Syntax is not valid.")


    #DROP TABLE Shippers;
    #DROP INDEX index_name;
    
    def drop(self,arg: str):
        arg = arg.rstrip(';')
        arg =  " ".join(arg.split())
        lower_arg = arg.lower()
        if lower_arg[:5] == 'table':
            table_name = arg[5:].strip()
        
            
            #TO DO: pass table_name to somewhere!!
            
            print(table_name)
            tables[table_name].drop_table()
            del tables[table_name]
            del metadata[table_name]
            
        elif lower_arg[:5] == 'index':
            print(arg[8:])
            
            l = arg[8:].find("(")
            r = arg[8:].find(")")
            
            table_name = arg[8:][:l].strip()
            column_name = arg[8:][l+1:r].strip()
            tables[table_name].drop_index(column_name)
                

    
    #INSERT INTO table_name (column1, column2, column3, ...) VALUES (value1, value2, value3, ...); 
    #INSERT INTO Customers (    Address, City, PostalCode, Country)     VALUES ('Skagen 21', 'Stavanger', 4006, 'Norway');
    def insert(self,arg: str):
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
 
        tables[table_name].add_record(values)
        
        
        
    #DELETE FROM Customers WHERE CustomerName == Alfreds Futterkiste AND ID > 4;
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
            #table_name = arg[from_position + 4:].strip()
            #print(table_name)
            # No where clase, pass table_name to somewhere !
            # TO DO ... 
            tables[table_name].delete_record()
            
        else:
            table_name = arg[from_position + 4: where_position].strip()
            if 'AND' in arg:
                
                conditions = arg[where_position + len('where'):].strip().split('AND')
            else:
                conditions = arg[where_position + len('where'):].strip().split('and')
    
            conditions = list(map(str.strip, conditions))
            where = []
            for condition in conditions:
                operators = ['==', '<>', '<', '>']
                no_operator = True
                for operator in operators:
                    if operator in condition:
                        op = operator
                        location = condition.find(op)
                        l_op = condition[:location].strip()
                        r_op = condition[location + len(op):].strip()
                        no_operator = False
                        break
                if no_operator:
                    raise Exception(f"no operator found in {condition}")
                r_op = auto_type(r_op)
                where.append({'symbol': op, 'column': l_op, 'condition': r_op})
            #print(where)
            #print(table_name)    
            print(where)
            

            tables[table_name].delete_record(where)

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
        if 'from' in arg:
            arg = arg.split('from')
        elif 'FROM' in arg:
            arg = arg.split('FROM')
        else:
            raise Exception ("Syntax is not correct. Must have FROM keyword and provide a table name.")
        
        
        # table_name = re.sub(' +', ' ', arg[1]).strip()[:re.sub(' +', ' ', arg[1]).strip().find(" ")]
        table_name = re.sub(' +', ' ', arg[1]).strip()
        
        
        
        
        position = table_name.find("order by")
        if position == -1:
            position_upper = table_name.find("ORDER BY")
            if position_upper == -1:
                pass
            else:
                table_name = table_name[:position_upper].strip()
        
        else:
            table_name = table_name[:position].strip()
            
            
            
        
            
            
            

        #Remove extra space 
        select_from = re.sub(' +', ' ', arg[0]).strip()
        
        select_from = strip_list_noempty(select_from.split(','))
        #print(select_from)
        
        columns = []
        aggregated_columns = []
        if '.' in select_from[0]:
            pattern = r'(.*)(\.)(.*)'
            for substr in select_from:
                pattern = r'(.*)(\.)(.*)'
                comp  = re.compile(pattern)
                new_ret = comp.findall((substr))
                for x in new_ret:
                    if "(" not in x[0]:
                        columns.append((x[0],x[2]))
                    
                    else: 
        
                        pl = x[0].find("(")
                        pr = x[2].find(")")
                 
                        if x[0][:pl].lower().strip() not in ["min", "avg", "max","count","sum"]:
                            raise Exception ("Aggregation function must be in one of these: min, avg, max, count and sum.")
                        aggregated_columns.append((x[0][:pl].lower().strip() ,x[0][pl+1:],x[2][:pr]))
                        
                    
                      
        
        
        elif "*" in select_from:
            columns= select_from
            #columns = ['*']
            
        else: 
            # No dots
            for x in select_from:
                if"(" not in x:
                    columns.append(x)
                else:
                     pl = x.find("(")
                     pr = x.find(")")
                     
                     if x[:pl].lower().strip() not in ["min", "avg", "max","count","sum"]:
                         raise Exception ("Aggregation function must be in one of these: min, avg, max, count and sum.")
                    
                     aggregated_columns.append((x[:pl].lower().strip(),x[pl+1:pr]))
                     
        #These two are done.             
        #print (columns)
        #print(aggregated_columns)   
        
        
        # Process where clause
        where = []
        where_statement= ""
        if 'WHERE' in re.sub(' +', ' ', arg[1]):
            where_statement = re.sub(' +', ' ', arg[1]).split('WHERE')[1]
            table_name=re.sub(' +', ' ', arg[1]).split('WHERE')[0].strip()
            print(table_name)
        elif 'where' in re.sub(' +', ' ', arg[1]):
            where_statement = re.sub(' +', ' ', arg[1]).split('where')[1]
            table_name=re.sub(' +', ' ', arg[1]).split('where')[0].strip()
            
            
        if len(where_statement) != 0:
            if "GROUP BY" in where_statement:
                where_statement = where_statement.split('GROUP BY')[0]
            elif "group by" in where_statement: 
                where_statement = where_statement.split('group by')[0]
            
            elif "ORDER BY" in where_statement:
                where_statement = where_statement.split('ORDER BY')[0]
            elif "order by" in where_statement: 
                where_statement = where_statement.split('order by')[0]
        
        
        # Customers.CustomerID > 3 and Customers.CustomerName = "Jenny";         
        # State <> NY 
            where_statement = self.filter_space(where_statement.split(" "))
            #['Customers.CustomerID', '>', '3', 'Customers.CustomerName', '=', '"Jenny";']
            #['State', '<>', 'NY']
             
             # No dot situation:
            if '.' in where_statement[0]:
                for i in range(0, len(where_statement),3):
                    table_column =  where_statement[i]
                    
                    pattern = r'(.*)(\.)(.*)'
                    comp  = re.compile(pattern)
                    new_ret = comp.findall(table_column)
                    
                    if '.' not in where_statement[i+2]:
                        where.append({'symbol': where_statement[i+1], 'table': new_ret[0][0],'column':new_ret[0][2], 'condition': auto_type(where_statement[i+2])})   
                    else: 
                        #'orders.agent_code'
                        p = where_statement[i+2].find(".")
                        where.append({'symbol': where_statement[i+1], 'table': new_ret[0][0],'column':new_ret[0][2], 'condition': ((where_statement[i+2][:p].strip(),where_statement[i+2][p+1:].strip()))}) 
                        
            
            if '.' not in where_statement[0]:
                for i in range(0, len(where_statement),3):
                    where.append({'symbol': where_statement[i+1], 'column': where_statement[i], 'condition': auto_type(where_statement[i+2])})
        else:
            if "GROUP BY" in where_statement:
                table_name = where_statement.split('GROUP BY')[0].strip()
            elif "group by" in where_statement: 
                table_name = where_statement.split('group by')[0].strip()
            
            elif "ORDER BY" in where_statement:
                table_name = where_statement.split('ORDER BY')[0].strip()
            elif "order by" in where_statement: 
                table_name = where_statement.split('order by')[0].strip()
                print('my table_name: '+table_name )
                    
        
        # Process join condition, only support joining on one table 
        # Only support one join! 
        join = []
        join_statement = ""
        if 'join' in re.sub(' +', ' ', arg[1]):
            join_statement = re.sub(' +', ' ', arg[1]).split('join')[1]
        elif 'JOIN' in re.sub(' +', ' ', arg[1]):
            join_statement = re.sub(' +', ' ', arg[1]).split('JOIN')[1]
        
        if len(join_statement) != 0:
            if "GROUP BY" in join_statement:
                join_statement = join_statement.split('GROUP BY')[0]
            elif "group by" in join_statement: 
                join_statement = join_statement.split('group by')[0]
            
        if len(join_statement) != 0:
            if "ORDER BY" in join_statement:
                join_statement = join_statement.split('ORDER BY')[0]
            elif "order by" in join_statement: 
                join_statement = join_statement.split('order by')[0]
               
            
        if len(join_statement) != 0:
            if "where" in join_statement:
                join_statement = join_statement.split('where')[0]
            elif "WHERE" in join_statement: 
                join_statement = join_statement.split('WHERE')[0]
        
        
        if len(join_statement) != 0:
            if "on" in join_statement:
                join_statement = join_statement.split('on')[1].strip()
            elif "ON" in join_statement:
                join_statement = join_statement.split('ON')[1].strip()
            else:
                raise Exception ("Syntax is not correct. Must have ON keyword for join statement.")
        
        
        
        if len(join_statement) != 0:
            if "=" in join_statement:
                join_statement = join_statement.split('=')
            else:
                raise Exception ("Syntax is not correct. Must have EQUAL keyword for join statement.")
        
        # ['Orders.order_id ', ' Customers.CustomerID']
        if len(join_statement) != 0:
            for x in join_statement:
                p = x.find(".")
                if p == -1:
                    raise Exception("Join syntax is not correct, must include dot.")
                
                join.append((x[:p].strip(),x[p+1:].strip()))
                     
        # Process group by, only support single column 
        
        group_by = []
        group_by_statement = ""
        if 'group by' in re.sub(' +', ' ', arg[1]):
            group_by_statement = re.sub(' +', ' ', arg[1]).split('group by')[1]
        elif 'GROUP BY' in re.sub(' +', ' ', arg[1]):
            group_by_statement = re.sub(' +', ' ', arg[1]).split('GROUP BY')[1]
        if len(group_by_statement) != 0:
            if "ORDER BY" in group_by_statement:
                group_by_statement = group_by_statement.split('ORDER BY')[0]
            elif "order by" in group_by_statement: 
                group_by_statement = group_by_statement.split('order by')[0]
        
        if len(group_by_statement) != 0:
            if "." in group_by_statement:
                p = group_by_statement.find(".")
                group_by.append((group_by_statement[:p].strip(),group_by_statement[p+1:].strip()))
            
            else:
                group_by.append(group_by_statement.strip())
                
        
        # Process order by, only support ordering by one column
        order_by = []
        aggregated_order_by = []
        order_by_statement = ""
        if 'order by' in re.sub(' +', ' ', arg[1]):
            order_by_statement = re.sub(' +', ' ', arg[1]).split('order by')[1]
        elif 'ORDER BY' in re.sub(' +', ' ', arg[1]):
            order_by_statement = re.sub(' +', ' ', arg[1]).split('ORDER BY')[1]
        
        #print(order_by_statement)  
        
        
        #Count(CustomerID) DESC
        #agents.agent_code
        #State DESC
        # SUM(Customers.CustomerID) DESC
        #AVG(Customers.CustomerID)
        
        if len(order_by_statement)!= 0:
            
            if "DESC" in order_by_statement:
                order_by_statement = order_by_statement.strip("DESC").strip()
                if "(" in order_by_statement:
                    if "." in order_by_statement:
                        l = order_by_statement.find("(")
                        func = order_by_statement[:l].strip().lower()
                        if func not in ["min", "avg", "max","count","sum"]:
                           
                            raise Exception ("Aggregation function must be in one of these: min, avg, max, count and sum.")
                
                        dot_position = order_by_statement.find(".")
                        table_name = order_by_statement[l+1:dot_position].strip()
                        print('order: '+table_name)
                        r = order_by_statement.find(")")
                        column_name =  order_by_statement[dot_position+1:r].strip()
                        aggregated_order_by.append(("desc",func,table_name,column_name))
            
                    
                    else:
                        
                        pl = order_by_statement.find("(")
                        pr = order_by_statement.find(")")
                     
                        if order_by_statement[:pl].lower().strip() not in ["min", "avg", "max","count","sum"]:
                                raise Exception ("Aggregation function must be in one of these: min, avg, max, count and sum.")
                        func = (order_by_statement[:pl].lower().strip())
                        aggregated_order_by.append(("desc",func,order_by_statement[pl+1:pr].strip()))
                        #[('desc', 'count', 'CustomerID')]
                        
                        
                else: 
                    # No aggregate function but contaisn dots
                    if "." in order_by_statement:
                        dot_position = order_by_statement.find(".")
                        table_name = order_by_statement[:dot_position].strip()
                        print('. : '+table_name)
                        column_name =  order_by_statement[dot_position+1:].strip()
                        order_by.append(("desc",table_name,column_name))
            
                    else:
                        #No aggregate function, no dots
                        order_by.append(("desc",order_by_statement))
                        
            else:
                order_by_statement = order_by_statement.strip()
                if "(" in order_by_statement:
                    if "." in order_by_statement:
                        l = order_by_statement.find("(")
                        func = order_by_statement[:l].strip().lower()
                        if func not in ["min", "avg", "max","count","sum"]:
                           
                            raise Exception ("Aggregation function must be in one of these: min, avg, max, count and sum.")
                
                        dot_position = order_by_statement.find(".")
                        table_name = order_by_statement[l+1:dot_position].strip()
                        print('(. order: '+table_name)
                        r = order_by_statement.find(")")
                        column_name =  order_by_statement[dot_position+1:r].strip()
                        aggregated_order_by.append(("asc",func,table_name,column_name))
                        
                    else:
                        
                        pl = order_by_statement.find("(")
                        pr = order_by_statement.find(")")
                     
                        if order_by_statement[:pl].lower().strip() not in ["min", "avg", "max","count","sum"]:
                                raise Exception ("Aggregation function must be in one of these: min, avg, max, count and sum.")
                        func = (order_by_statement[:pl].lower().strip())
                        aggregated_order_by.append(("asc",func,order_by_statement[pl+1:pr].strip()))
                        #[('desc', 'count', 'CustomerID')]
                        
                        
                else: 
                    # No aggregate function but contaisn dots
                    if "." in order_by_statement:
                        dot_position = order_by_statement.find(".")
                        table_name = order_by_statement[:dot_position].strip()
                        print('. order: '+table_name)
                        column_name =  order_by_statement[dot_position+1:].strip()
                        order_by.append(("asc",table_name,column_name))
            
                    else:
                        #No aggregate function, no dots
                        order_by.append(("asc",order_by_statement))    


        
        
        for element in columns:
            if element == "*":
                tables[table_name].select(['*'])
                return 
   
        
        if join==[]: # no join
            join=None
            if order_by==[]:
                order_by=None
                direct='asc'
            else:
                if len(order_by[0])==2:
                    direct=order_by[0][0]
                    order_by=[order_by[0][1]]         
                else:
                    direct='asc'
            if aggregated_columns==[]:
                aggregated_columns=None
            else:
                for i,tup in enumerate(aggregated_columns):
                    aggregated_columns[i]=(tup[1],tup[0])
            if group_by==[]:
                group_by=None
            else:
                group_by=[group_by[0][0]+'.'+group_by[0][1]]
            if where==[]:
                where=None
                
                       
        else: # join
            for i,tup in enumerate(columns):
                columns[i]=tup[0]+'.'+tup[1]
            # for i,tup in enumerate(join):
            #     join[i]=(tup[0]+'.'+tup[1])
            if order_by==[]:
                order_by=None
                direct='asc'
            else:
                if len(order_by[0])==2:
                    order_by=[order_by[0][0]+'.'+order_by[0][1]]
                    direct='asc'
                else:
                    direct=order_by[0][0]
                    order_by=[order_by[0][1]+'.'+order_by[0][2]]
            if aggregated_columns==[]:
                aggregated_columns=None
            else:
                for i,tup in enumerate(aggregated_columns):
                    aggregated_columns[i]=(tup[1]+'.'+tup[2],tup[0])
            if where==[]:
                where=None
            else:
                for i,dict in enumerate(where):
                    where[i]['column']=dict['table']+'.'+dict['column']
                    del where[i]['table']
                    if isinstance(dict['condition'],tuple):
                        where[i]['condition']=dict['condition'][0]+'.'+dict['condition'][1]
            if group_by==[]:
                group_by=None
            else:
                group_by=[group_by[0][0]+'.'+group_by[0][1]]
            
        

        # process join
        if join == None:
        
            
            if aggregated_columns:
                for element in aggregated_columns:
                    columns.append(element[0])
                    
            
            tables[table_name].select(columns, aggregated_columns, group_by,where, order_by,direct)
                

        
        else:
            print (join)
            
            print(join[0][0])
            print(join[1][1])
            Table.create_from_join(tables[join[1][0]],tables[join[1][0]],join[0][1], join[1][1]).select(columns, aggregated_columns, group_by,where, order_by,direct)
            
            
            
        

        

                   

    
   # UPDATE Customers SET ContactName =='Juan' where City == 'New';
    # UPDATE Customers SET ContactName ='Alfred Schmidt', City='Frankfurt' WHERE CustomerID == 1 and Name = "das";
    #UPDATE Customers SET ContactName == 'Juan' where City == 'New' and ID == 3;
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

                         
                
                
                pattern = r'(.*) (==|<|>|<>) (.*)'
                comp  = re.compile(pattern)
                new_ret = comp.findall(column_specification)
                columns_to_be_updated.append(new_ret[0][0])
                values_to_be_updated.append(auto_type(new_ret[0][2]))
            

            tables[table_name].update_record(columns_to_be_updated,values_to_be_updated)
                
        else:
            table_name = arg[: location_set].strip()
            
            if 'AND' in arg:
                
                conditions = arg[location_where + len('where'):].strip().split('AND')
            else:
                conditions = arg[location_where + len('where'):].strip().split('and')
            
            conditions = list(map(str.strip, conditions))

         

            where = []
            for condition in conditions:
                operators = ['==', '<>', '<', '>']
                no_operator = True
                for operator in operators:
                    if operator in condition:
                        op = operator
                        location = condition.find(op)
                        l_op = condition[:location].strip()
                        r_op = condition[location + len(op):].strip()
                        no_operator = False
                        break
                if no_operator:
                    raise Exception(f"no operator found in {condition}")

                r_op = auto_type(r_op)

                where.append({'symbol': op, 'column': l_op, 'condition': r_op})
                




            arg = arg[:location_where]
            pattern = r'(.*) (SET|set) (.*) '
            comp = re.compile(pattern)
            ret = comp.findall("".join(arg))

     
            column_specifications = ret[0][2].split(',')





            
            for column_specification in column_specifications:
                print(column_specification)
                
                pattern = r'(.*) (==) (.*)'
                comp  = re.compile(pattern)
                new_ret = comp.findall(column_specification)
                print(new_ret)
              
                columns_to_be_updated.append(new_ret[0][0])
                values_to_be_updated.append(auto_type(new_ret[0][2]))

            tables[table_name].update_record(columns_to_be_updated,values_to_be_updated,where)
         
        
 
    def exit(self,arg: str):
        print('exit!')



action_map = {
            'create': SQLParser().create,
            'CREATE':SQLParser().create,
            'drop': SQLParser().drop,
            'DROP': SQLParser().drop,
            'select': SQLParser().select,
            'SELECT': SQLParser().select,
            'insert': SQLParser().insert,
            'INSERT': SQLParser().insert,
            'delete': SQLParser().delete,
            'DELETE': SQLParser().delete,
            'UPDATE': SQLParser().update,
            'update': SQLParser().update
        }

class Runner(Cmd):
    
    prompt = "Mydb> "
    
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
        SQLParser().drop(arg)
    
  
            
    def do_DROP(self,arg:str):
        
        SQLParser().drop(arg)

       
    

    def do_select(self,arg:str):
        SQLParser().select(arg)

            
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
            
    def do_exec(self, arg: str): 

        i = 1
        try:
            f = open(arg.strip(';').strip(), 'r')
            while 1:
                line = f.readline().strip()
                if line == '':
                    break
                if line[0] == '#':
                    i += 1
                    continue
                action = line[:line.find(' ')]
                arg = line[line.find(' '):]
                action_map[action](arg)
                i += 1
        except Exception as e:
            print(f" An exception occurred at line {i}:")
            print(e)

    def do_LIST(self, arg:str):

        for table in tables:
            print(tables[table].table_name)

    def do_list(self, arg:str):

        for table in tables:
            print(tables[table].table_name)

    def do_COLUMNS(self, arg:str):
        if arg.split(" ")[0].lower() == "from":
            if arg.split(" ")[1] in tables:
                tables[arg.split(" ")[1]].show_columns()

    def do_columns(self, arg:str):
        if arg.split(" ")[0].lower() == "from":
            if arg.split(" ")[1] in tables:
                tables[arg.split(" ")[1]].show_columns()

    def do_quit(self,arg:str):
        print("See you.")
        return True

    def default(self, line: str):
        print(f"Unknown command: {line.split(' ')[0]}")



def save_metadata():
    with open ('metadata.json','w') as metafile:
        
        #Erase the meta_data_file e
        metafile.truncate(0)
        json.dump(metadata, metafile)
        


def load_metadata():

    if not os.path.exists('metadata.json'):
        with open('metadata.json', 'w') as myfile:
             pass
    else:
        
        with open('metadata.json', 'r+') as myfile:
            #data = myfile.read()
            json_data = json.load(myfile)
            for key, value in json_data.items():

                tables[key] = Table(key,value[1],value[2])
                
                metadata[key] =[key,value[1],value[2]]
                

        
        
        
        
            
        
#create table persons( ID int, lastname varchar, City varchar, PRIMARY KEY (ID));
#create table students( ID int, lastname varchar, Grade varchar, PRIMARY KEY (ID));

# ["persons", {"ID": "int", "lastname": "varchar", "City": "varchar"}, ["ID"]]

if __name__ == "__main__":


    load_metadata()
    Runner().cmdloop()
    
    save_metadata()
    


