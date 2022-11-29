import configparser
import os

from .. import pysqldb3 as pysqldb
from . import helpers

config = configparser.ConfigParser()
config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")

db = pysqldb.DbConnect(db_type=config.get('PG_DB', 'TYPE'),
                       host=config.get('PG_DB', 'SERVER'),
                       db_name=config.get('PG_DB', 'DB_NAME'),
                       username=config.get('PG_DB', 'DB_USER'),
                       password=config.get('PG_DB', 'DB_PASSWORD'))

sql = pysqldb.DbConnect(db_type=config.get('SQL_DB', 'TYPE'),
                        host=config.get('SQL_DB', 'SERVER'),
                        db_name=config.get('SQL_DB', 'DB_NAME'),
                        # ldap=True,
                        use_native_driver=True,
                        username=config.get('SQL_DB', 'DB_USER'),
                        password=config.get('SQL_DB', 'DB_PASSWORD'))

sql2 = pysqldb.DbConnect(type=config.get('SECOND_SQL_DB', 'TYPE'),
                        host=config.get('SECOND_SQL_DB', 'SERVER'),
                        db_name=config.get('SECOND_SQL_DB', 'DB_NAME'),
                        # ldap=True,
                        username=config.get('SECOND_SQL_DB', 'DB_USER'),
                        password=config.get('SECOND_SQL_DB', 'DB_PASSWORD'),
                        use_native_driver=True)

def test_connect():
    print(db)
    print(sql)
    print(sql2)
