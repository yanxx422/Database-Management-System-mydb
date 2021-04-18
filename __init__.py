import re
from cmd import Cmd

#import SQLParser

def create(arg: str):
    print('Create!')

def drop(arg: str):
    print('Drop!')  
    #Remove extra white spaces
    arg =  " ".join(arg.split())
    print(arg)
    
    #Extract the table name
    arg = arg[5:].strip()
    
    
    
    print(arg)


def insert(arg: str):
    print('insert!')  
    
def delete(arg: str):
    print('delete!')

#"WHERE" is not implemented
def select(arg:str):
    arg =  " ".join(arg.split()).lower()

    print(arg)

    # index of the from 
    from_postion = arg.find('from')

    attributes = arg[:from_postion].strip().split(',')

    #Extract the attributes name, attributes is a list of string 
    attributes = [x.strip(' ') for x in attributes]
    print(attributes)


    # Extract the table name 
    table_name = arg[from_postion + len('from'):].strip()
    print(table_name)    
    
  
def show(arg: str):
    print('show!')

def exit(arg: str):
    print('exit!')


class Engine(Cmd):
    def __init__(self):
        Cmd.__init__(self)

    def do_create(self,arg:str):
        try:
            create(arg)
        except Exception as e:
            print('Creating Failed.', e)

    def do_drop(self,arg:str):
        try:
            drop(arg)
        except Exception as e:
            print('Dropping Failed.', e)
            
     def do_select(self,arg:str):
        try:
            select(arg)
        except Exception as e:
            print('Selecting Failed.', e)
    def do_insert(self,arg:str):
        try:
            insert(arg)
        except Exception as e:
            print('Inserting Failed.', e)

    def do_delete(self,arg:str):
        try:
            delete(arg)
        except Exception as e:
            print('Deleting Failed.', e)

    def do_show(self,arg:str):
        try:
            show(arg)
        except Exception as e:
            print('Showing Failed.: ', e)

    def do_exit(self,arg:str):
        print("See you.")
        return True

    def default(self, line: str):
        print(f"Unknown command: {line.split(' ')[0]}")

if __name__ == "__main__":
    Engine().cmdloop()
