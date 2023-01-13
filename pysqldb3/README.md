## Pysqldb3 Library Components 

## 1. Pysqldb3 (DbConnect) - Pysqldb3.py
Pysqldb contains DbConnect, a class built off of pymssql/psycopg2 that makes it easy to connect to Postgres/Sql Server databases in Python. 
### 1.1 Simple Example/Setup  
To create a DbConnect object:
1. Import 
   `from pysqldb3 import pysqldb3`
2. `db = pysqldb.DbConnect(user=None, password=None, ldap=False, type=None, server=None, database=None, port=5432,
                 allow_temp_tables=False, use_native_driver=True, default=False, quiet=False))` 
    + Replace server_name and db_name with the actual values. 
    + You must specify the type, either `PG` (Postgres) or `MS` (SQL Server). 
    + You can also add your username using `user='my_username'` 
    + You can add your password similarly using `password='mypassword'`
    + Other inputs include LDAP and ports. If you're unsure about these, feel free to ask around. 
3. Now, you can query on your database by using `db.query('select * from my_schema.my_table')`

### 1.2 Functions 
In Jupyter or Python shell, use help(pysqldb) to show all public functions and their inputs. <br>
#### pysqldb3 public functions:


1. [`DbConnect`](#connect): Connects to database


### DbConnect
**`pysqldb3.DbConnect.(quiet=False)`**
Creates database connection instance.  
###### Parameters:
 - **`quiet`: bool, default False**: When true, does not print database connection information.
 
 
**Sample** 
```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(quiet=False)
>>> print(db)

Additional database connection details required:
Database type (MS/PG)PG
Server:dotdevrhpgsql01
Database name:ris
User name (ris):hshi
Password (ris): ********

Database connection (PG) to ris on dotdevrhpgsql01 - user: hshi
Connection established 2023-01-13 09:41:52, /
- ris version 0.0.3 -
```
[Back to Table of Contents](#pysqldb3-public-functions)
<br>

### disconnect
**`DbConnect.disconnect(quiet=False)`**
Disconnects from database. When called this will print database connection information and closed timestamp. 
###### Parameters:
 - **`quiet`: bool, default False**: When true, does not print database connection information. 

**Sample**
```
>>> from pysqldb3 import pysqldb3
>>> db = pysqldb3.DbConnect(quiet=False)
...
>>> db.disconnect()
Database connection (PG) to ris on dotdevrhpgsql01 - user: hshi
Connection closed 2023-01-13 10:07:15

```
[Back to Table of Contents](#pysqldb-public-functions)
<br>

### check_conn
**`DbConnect.check_conn()`**
Checks and reconnects to connection if not currently connected.

###### Parameters: 
- None

**Sample**
```
>>> from ris import pysqldb
>>> db = pysqldb.DbConnect(type='pg', server=server_address, database='ris', user='shostetter', password='*******')
>>> db.check_conn()

```
[Back to Table of Contents](#pysqldb-public-functions)
<br>