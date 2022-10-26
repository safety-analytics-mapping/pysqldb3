import os
import pytest
from .. import pysqldb3 as pysqldb
from ..Config import read_config, write_config

target_sections_keys = {'ODBC Drivers': ['ODBC_DRIVER','NATIVE_DRIVER'],
                    'DEFAULT DATABASE': ['type','server','database'],
                    'GDAL DATA': ['GDAL_DATA']}
testing_keys = ['TYPE','SERVER','DB_NAME','DB_USER','DB_PASSWORD']
testing_sections = {'PG_DB': testing_keys, 'SECOND_PG_DB': testing_keys, 'SQL_DB': testing_keys, 'SECOND_SQL_DB': testing_keys}
config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.cfg')
test_config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tests\\db_config.cfg')
config_file_2 = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config2.cfg')
test_config_file_2 = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'tests\\db_config2.cfg')

class TestConfigIO:

    @pytest.mark.issue8
    def test_read_default_config(self):
        # Test
        cfg_sections = read_config(config_file)

        # Assert correct sections
        assert cfg_sections.keys() == target_sections_keys.keys()
        
        # Assert correct keys in each section
        for sec in target_sections_keys:
            assert cfg_sections.get(sec) == target_sections_keys.get(sec)

    @pytest.mark.issue8
    def test_write_default_config(self):
        # assert config2.cfg doesn't exist
        assert not os.path.exists(config_file_2)

        # test
        write_config(config_file_2)
        assert os.path.exists(config_file_2)
        
        # assert correct sections and keys
        cfg_sections = read_config(config_file_2)
        assert cfg_sections.keys() == target_sections_keys.keys()
        for sec in target_sections_keys:
            assert cfg_sections.get(sec) == target_sections_keys.get(sec)
        
        # cleanup
        os.remove(config_file_2)

    @pytest.mark.issue8
    def test_read_testing_config(self):
        # Test
        cfg_sections = read_config(test_config_file)

        # Assert correct sections
        assert cfg_sections.keys() == target_sections_keys.keys()
        
        # Assert correct keys in each section
        for sec in target_sections_keys:
            assert cfg_sections.get(sec) == target_sections_keys.get(sec)

    @pytest.mark.issue8
    def test_write_testing_config(self):
        # assert db_config2.cfg doesn't exist
        assert not os.path.exists(test_config_file_2)

        # test
        write_config(test_config_file_2)
        assert os.path.exists(test_config_file_2)
        
        # assert correct sections and keys
        target_sections_keys = read_config(test_config_file_2)
        assert target_sections_keys.keys() == target_sections_keys.keys()
        for sec in target_sections_keys:
            assert target_sections_keys.get(sec) == target_sections_keys.get(sec)
        
        # cleanup
        os.remove(test_config_file_2)