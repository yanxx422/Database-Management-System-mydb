from enum import Enum
import json

class ColumnConstraints(Enum):
    PRIMARY = 'PRIMARY KEY'
    UNIQUE = 'UNIQUE'
    NOT_NULL = 'NOT NULL'
    NULL = 'NULL'



class ColumnType(Enum):
    INT = int = 'int'
    VARCHAR = varchar = 'str'
    FLOAT= float = 'float'
    BOOL = bool = 'bool'


TYPE_MAP = {
    'int':int,
    'float':float,
    'str': str,
    'INT':int,
    'FLOAT': float,
    'VARCHAR': str,
    'BOOL':bool,
    'bool':bool
}

class Interface():

    json = json

    def serialized(obj):
        raise NotImplementedError

    def serialized(self):
        raise NotImplementedError


class Column(Interface):

    def __init__(self, data_type, constraints = "NULL", default = None):
        self.__type = data_type
        self.__constraints = constraints
        self.__default = default
        self.__values = []
        self.__rows = 0

        if not isinstance(self.__constraints, list):
            self.__constraints = [self.__constraints]

        if not (self.__type in ColumnType.__members__):
            raise TypeError ('Data type is not valid')

        for constraint in self.__constraints:
            if not (constraint in ColumnConstraints.__members__):
                raise TypeError('Constraint type is not valid.')

        if self.__default is not None and "UNIQUE" in self.__constraints:
            raise Exception ('Unique constraint is not allowed to set default null value.')


    def check_index(self,index):
        if not isinstance(index,int) or not -index < self.__rows > index:
            raise Exception ('Index not valid, not this element.')
        return True


    def check_type(self,value):
        if value is not None and not isinstance(value, TYPE_MAP[self.__type]):
            raise TypeError('data type error, value must be %s' % self.__type)

    def check_constraints(self,value):
        # Data Entry must be unique for primary and unique constraints
        if "PRIMARY" in self.__constraints or "Unique" in self.__constraints:
            if value in self.__values:
                raise Exception('value %s exists' % value)

        if ("PRIMARY" in self.__constraints or "NOT NULL" in self.__constraints) and value is None:
            raise Exception('Column Not Null')
        return value


    def length(self):
        return self.__rows

    def get_type(self):
        return self.__type

    def get_constraints(self):
        return self.__constraints

    def get_data(self,index = None):

        #If index is an int, return the specific data
        if index is not None and self.check_index(index):
            return self.__values[index]

        #Otherwise, return all the deta of the column
        return self.__values

    def add_data(self, value):
        #If inserting empty data, then set it to defalt value
        if value is None:
            value = self.__default

        value = self.check_constraints(value)

        self.check_type(value)

        self.__values.append(value)

        self.__rows += 1

    def modify_data(self, index, value):

        self.check_index(index)

        value = self.check_constraints(value)

        self.check_type(value)

        self.__values[index] = value

    def delete_data(self, index):

        self.check_index(index)

        self.__values.pop(index)

        self.__rows -= 1


    # Convert the object to data
    def serialized(self):
        return Interface.json.dumps({
            'constraint': self.__constraints,
            'type': self.__type.value,
            'values': self.__values,
            'default': self.__default
        })

    #Convert the data to the object
    def deserialized(data):
        json_data = Interface.json.loads(data)

        constraints = [constraint in json_data['constraint']]

        obj = Column(ColumnType(json_data['type']), constraints, defualt = json_data['default'])

        for value in json_data['values']:
            obj.add(value)

        #Return the json object
        return obj





if __name__ == '__main__':


    name = Column("VARCHAR")

    #add_data(self, 3)

#   add_data(self, "Mike")

    name.add_data("Jenny")

    name.add_data("Tom")

    print(name.get_data())


    name.modify_data(1,"Jim")
    name.add_data("Bazinga")

    print(name.get_data())

    # This is a type error
    '''
    name.add_data(2)

    print(name.get_data())

    '''

    name.delete_data(2)
    print(name.get_data())

    name2 = Column("INT","PRIMARY")

    # This is a constraint error
    '''
    name2.add_data(1)
    name2.add_data(1)

    print(name2.get_constraints())
    '''


