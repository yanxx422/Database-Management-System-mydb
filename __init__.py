import re
from cmd import Cmd

import SQLParser

class Engine(Cmd):
    def __init__(self):
        Cmd.__init__(self)

    def do_create(self,arg:str):
        try:
            create(arg):
        except Exception ('Creating Failed.')

    def do_drop(self,arg:str):
        try:
            drop(arg):
        except Exception ('Dropping Failed.')

    def do_insert(self,arg:str):
        try:
            insert(arg):
        except Exception ('Inserting Failed.')

    def do_delete(self,arg:str):
        try:
            delete(arg):
        except Exception ('Deleting Failed.')

    def do_show(self,arg:str):
        try:
            show(arg):
        except Exception ('Showing Failed.')

    def do_exit(self,arg:str):
        print("See you.")
        return True

    def default(self, line: str):
        print(f"Unknown command: {line.split(' ')[0]}")

if __name__ == "__main__":
    Engine().cmdloop()
