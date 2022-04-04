# Databse Management System

This is an implementation of a database management system in Python. The user interacts with the system via SQL commands and queries.


## Design decisions

At the most basic level, the program can be partitioned into the interpretation of the SQL language and an underlying data management structure. The below structure breaks this down even further, however, in broader strokes, runner and SQLParser classes are on the user interaction/SQL language side of the program, while the table, column and index class are the structures that manage the data.

Structure and Scopes: 

   Main
       | 
	Runner class
	       |
	SQLParser class
  	      |
	Table class ------ Index class
      |
Column class  

we pursued a record based paradigm, whereby a record is implemented as a tuple. The tuples are stored in a Python shelve, which is a persistent data store with an interface identical to a Python dictionary. Critically, the mapping is from an md5 hash of the record to the record. This makes it possible to determine in O(1) time whether a record is already in the table. It also makes locating a record as fast. 

The main data management structure is the table class. It contains a shelve that maps all of the tuples. It also contains a list of column objects, each keeping track of the legal data_type of the attribute. Additionally, it contains a list of index objects.

Index objects contain, aside from basic information about the attribute the index refers to, a dictionary mapping attribute values to the hash of the record. This implementation means that while the large amounts of record data are stored on disk, the relatively smaller index mappings are stored in memory and can be rapidly accessed.

## Sample Functionalities Syntax

### SELECT 
`select Orders.order_id, Customers.CustomerName  from Customers join Orders ON Orders.order_id  = Customers.CustomerID where Customers.CustomerID  > 3 and  Customers.CustomerName = “Jenny”;`

`select Orders.order_id, Customers.CustomerName  from Customers join Orders ON Customers.CustomerID = Orders.order_id   where Customers.CustomerID  > 3 and  Customers.CustomerName = “Jenny”;`

`select id,name  from students where id > 3 and  name = Jerry`

`select *  from students where id > 3 and  name = Jerry`

`select id,   name  from students`

### CREATE
`create table persons( ID int, lastname VARCHAR, City VARCHAR, PRIMARY KEY (ID));`

`create table persons( ID int, lastname VARCHAR, City VARCHAR);`

`REATE INDEX index_name ON tableName (tableColumn);` (Only support single index creation).

### DROP 

`DROP table students;`
`DROP INDEX index_name;`

### INSERT

`INSERT INTO Customers (    Address, City, PostalCode, Country)     VALUES ('Skagen 21', 'Stavanger', 4006, 'Norway');`

### UPDATE

`UPDATE Customers SET ContactName = 'Juan';`

`UPDATE Customers SET ContactName = 'Alfred Schmidt', City = 'Frankfurt';`

`UPDATE Customers SET ContactName = 'Juan' WHERE Country = 'Mexico';`

`UPDATE Customers SET ContactName = 'Juan', City  =  'Frankfurt' WHERE Country = 'Mexico' AND Country_ID = 2;`

 (MUST HAVE WHITE SPACE close to ‘= ‘)

### DELETE
`DELETE FROM Customers WHERE CustomerName = Alfreds Futterkiste AND ID = 4;`
`DELETE FROM Customers WHERE CustomerName=Alfreds;`
`DELETE FROM Customers;`

## Deficiencies 
- For some queries, the parser may not recognize white spaces in the table name provided.
- Only support joining on one table, only support order_by and group_by on one attribute.
- By default, we do inner join, we don’t recognize “INNER JOIN” or “LEFT JOIN”. (Will work on this after other parts have been finished. 
- Don’t support “like” statements.
- For constraints, we only support “primary key”, and will keep working on “unique” if we have time left.
- Don’t support “Having” statements.
- When we search based on a range, b plus tree is more efficient than hash table structure. But since we couldn’t find a stable tree package that does the disk management, it is required that we implement it on our own if we want to make it faster. Since we tried different approaches and those took a substantial amount of time, we decided not to pursue tree structure.  

## Credits

This is a collaborate project with me and my classmate at Georgetown U database class, Rashid Haddad.  
