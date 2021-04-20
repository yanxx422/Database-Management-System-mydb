import re
from cmd import Cmd


class SQLParser:
    def __init__(self):
        self = self 
        

    def create(self,arg: str):
        print('Create!')

    def drop(self,arg: str):
        print('Drop!')  
        #Remove extra white spaces
        arg =  " ".join(arg.split())
        print(arg)
    
        #Extract the table name
        arg = arg[5:].strip()
    
    
    
        print(arg)


    def insert(self,arg: str):
        print('insert!')  
    
    def delete(self,arg: str):
        print('delete!')


    def filter_space(self, obj):
        ret = []
        for x in obj:
            if x.strip() == '' or x.strip() == 'AND':
                continue
            ret.append(x)

        return ret
    
    def parse(self,arg: str):
        # arg = 'name , id from students where id = 6'
        if 'where' in arg:
            arg = arg.split('where')
        else:
            arg = arg.split('WHERE')
        # arg = ['name , id from students ', ' id = 6']
        
        base_statement = self.filter_space(arg[0].split(" "))
        # base statement = ['name,id', 'from', 'students']
        
        conditions = None 
        
        if len(arg) == 2:
            conditions = self.filter_space(arg[1].split(" "))           
            # conditions = ['id', '=', '6']


        return base_statement,conditions 
    
        

    
    def select(self,arg:str):
        # base_statement =  ['id,', 'name', 'from', 'students']
        base_statement,conditions = self.parse(arg)
        
        pattern = r'(.*) (FROM|from) (.*)'
        
        comp = re.compile(pattern)
        ret = comp.findall(" ".join(base_statement))
        # ret =  [('id, name', 'from', 'students')]
        
        if ret and len(ret[0]) == 3:
            columns = ret[0][0]
            table_name = ret[0][2]
            
            if columns != '*':
                columns = [column.strip() for column in columns.split(",")]
                #Columns =  ['id', 'name']
                
            if conditions:
                if 'and' in conditions:
                    conditions.remove('and')
                if 'AND' in conditions:
                    conditions.remove('AND')
                where = []
                for i in range(0, len(conditions),3):
                    where.append({'symbol': conditions[i+1], 'column': conditions[i], 'condition': conditions[i+2]})
                    
                
                print(where)
                        

        
        # pass columns, table_name and where to somewhere 
        
        
     
  
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

    def do_drop(self,arg:str):
        try:
            SQLParser().drop(arg)
        except Exception as e:
            print('Dropping Failed.', e)
            
    def do_select(self,arg:str):
        try:
            SQLParser().select(arg)
        except Exception as e:
            print('Selecting Failed.', e)
            
    def do_insert(self,arg:str):
        try:
            SQLParser().insert(arg)
        except Exception as e:
            print('Inserting Failed.', e)

    def do_delete(self,arg:str):
        try:
            SQLParser().delete(arg)
        except Exception as e:
            print('Deleting Failed.', e)

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
