import configparser
import os

from .. import pysqldb3 as pysqldb
from . import helpers

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

db = pysqldb.DbConnect(type=config.get('PG_DB', 'TYPE'),
                       server=config.get('PG_DB', 'SERVER'),
                       database=config.get('PG_DB', 'DB_NAME'),
                       user=config.get('PG_DB', 'DB_USER'),
                       password=config.get('PG_DB', 'DB_PASSWORD'))

if config.get('SQL_DB','LDAP').lower() == 'true':
    sql = pysqldb.DbConnect(type=config.get('SQL_DB','TYPE'),
                            server=config.get('SQL_DB','SERVER'),
                            database=config.get('SQL_DB','DB_NAME'),
                            ldap=True,
                            use_native_driver=True)
else:
    sql = pysqldb.DbConnect(type=config.get('SQL_DB', 'TYPE'),
                            server=config.get('SQL_DB', 'SERVER'),
                            database=config.get('SQL_DB', 'DB_NAME'),
                            user=config.get('SQL_DB', 'DB_USER'),
                            password=config.get('SQL_DB', 'DB_PASSWORD'),
                            use_native_driver=True)


sql2 = pysqldb.DbConnect(type=config.get('SECOND_SQL_DB', 'TYPE'),
                        server=config.get('SECOND_SQL_DB', 'SERVER'),
                        database=config.get('SECOND_SQL_DB', 'DB_NAME'),
                        # ldap=True,
                        user=config.get('SECOND_SQL_DB', 'DB_USER'),
                        password=config.get('SECOND_SQL_DB', 'DB_PASSWORD'),
                        use_native_driver=True)

def test_connect():
    print(db)
    print(sql)
    print(sql2)
