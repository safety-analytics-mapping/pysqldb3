import os
import configparser

from .. import pysqldb3 as pysqldb

test_config = configparser.ConfigParser()
test_config.read(os.path.dirname(os.path.abspath(__file__)) + "\\db_config.cfg")


def test():
    db = pysqldb.DbConnect(default=True, password=test_config.get('PG_DB', 'DB_PASSWORD'),
                           user=test_config.get('PG_DB', 'DB_USER'))
    assert True

    sql = pysqldb.DbConnect(type=test_config.get('SQL_DB', 'TYPE'),
                            server=test_config.get('SQL_DB', 'SERVER'),
                            database=test_config.get('SQL_DB', 'DB_NAME'),
                            user=test_config.get('SQL_DB', 'DB_USER'),
                            password=test_config.get('SQL_DB', 'DB_PASSWORD'))

    assert True

    azure = pysqldb.DbConnect(type='azure',
                                server= 'rg-azu-e2-dot-synapse-dev-ws-ondemand.sql.azuresynapse.net',
                                database='SIRTA',
                                user=test_config.get('PG_DB', 'DB_USER')
                                )

    assert True
