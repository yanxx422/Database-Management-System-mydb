import re
from cmd import Cmd

import SQLParser

class Engine(Cmd):
    def __init__(self):
        Cmd.__init__(self)

    def action_create(self:str):
        try:
            create(arg):
        except Exception ('Creating Failed.')

    def action_drop(self,arg:str):
        try:
            drop(arg):
        except Exception ('Dropping Failed.')

    def action_insert(self,arg:str):
        try:
            insert(arg):
        except Exception ('Inserting Failed.')

    def action_delete(self,arg:str):
        try:
            delete(arg):
        except Exception ('Deleting Failed.')

    def action_show(self,arg:str):
        try:
            show(arg):
        except Exception ('Showing Failed.')

    def action_exit(self,arg:str):
        print("See you.")
        return True

    def default(self, line: str):
        print(f"Unknown command: {line.split(' ')[0]}")

if __name__ == "__main__":
    Engine().cmdloop()
