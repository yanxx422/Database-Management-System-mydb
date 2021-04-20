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
            if x.strip() == '' or x.strip() == 'AND' or x.strip() == 'and':
                continue
            ret.append(x)

        return ret




# select id,name  from students join students2 ON students2.id = students.id where id > 3 and  name = Jerry
# select id,name  from students where id > 3 and  name = Jerry


    def select(self,arg:str):
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
            join = []
            
            if 'JOIN'in ret[0][2] or 'join'in ret[0][2]:
                # For now only deal with one join condition, and not including "and" in join condition
                pattern = r'(.*) (JOIN|join) (.*) (ON|on) (.*)(\.)(.*)(=|>|<|>=|<|<=|<>) (.*)(\.)(.*)'
                comp  = re.compile(pattern)
                new_ret = comp.findall((ret)[0][2])
                
                join.append({'another_table': new_ret[0][2], 'original_table_column':new_ret[0][6], 'another_table_column':new_ret[0][10], 'operator':new_ret[0][7]})
                
                print(join)
                table_name = new_ret[0][0]
            
            else:
                
                table_name = ret[0][2]
            
            if columns != '*':
                columns = [column.strip() for column in columns.split(",")]
                #Columns =  ['id', 'name']
                
            if len(arg) == 2:
                    conditions = self.filter_space(arg[1].split(" "))           
                    # conditions = ['id', '=', '6']
            if conditions:
                where = []
                for i in range(0, len(conditions),3):
                    where.append({'symbol': conditions[i+1], 'column': conditions[i], 'condition': conditions[i+2]})

                print(columns)
                print(table_name)
                print(where)
                print(join)



        # pass columns, table_name, where, join to some function  




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
