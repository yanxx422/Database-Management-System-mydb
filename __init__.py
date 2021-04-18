import re
from cmd import Cmd

#import SQLParser

def create(arg: str):
    print('Create!')

def drop(arg: str):
    print('Drop!')  


def insert(arg: str):
    print('insert!')  
    
def delete(arg: str):
    print('delete!')

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
            print('Creating Failed.: ', e)

    def do_drop(self,arg:str):
        try:
            drop(arg)
        except Exception as e:
            print('Dropping Failed.: ', e)
    def do_insert(self,arg:str):
        try:
            insert(arg)
        except Exception as e:
            print('Inserting Failed.: ', e)

    def do_delete(self,arg:str):
        try:
            delete(arg)
        except Exception as e:
            print('Deleting Failed.: ', e)

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
