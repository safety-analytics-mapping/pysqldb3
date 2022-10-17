import os
import pyodbc
import re
from collections import defaultdict


class SqlDriver:
    def __str__(self):
        return 'Getting latest ODBC drivers in the system'

    def __init__(self):
        self.odbc_driver = None
        self.native_driver = None
        print(self.__str__())
        self.get_latest_drivers()
        # self.write_odbc_to_config()

    @staticmethod
    def choose_max(driver_list):
        """
        Ugly but functional
        todo:  rewrite
        :param driver_list:
        :return:
        """
        if len(driver_list) == 1:
            return driver_list[0]
        x = r'(?!RDA)\s+[0-9]+\s*'
        mx = 0

        numbers = [re.findall(x, _) for _ in driver_list]

        for _ in numbers:
            if int(_[0])>mx:
                mx=int(_[0])

        return [i for i in driver_list if str(mx) in i][0]

    @staticmethod
    def get_drivers():
        odbc_drivers = list()
        native_drivers = list()
        if 'ODBC Driver' not in pyodbc.drivers():
            odbc_drivers.append('SQL Server')
        for i in pyodbc.drivers():
            if 'ODBC Driver' in i:
                odbc_drivers.append(i)
            elif 'SQL Server Native Client' in i:
                native_drivers.append(i)
        return odbc_drivers, native_drivers

    def get_latest_drivers(self):
        odbc_drivers, native_drivers = self.get_drivers()
        self.odbc_driver = self.choose_max(odbc_drivers)
        self.native_driver = self.choose_max(native_drivers)


def get_gdal_data_path():
    return os.environ["GDAL_DATA"]


def read_config(config_path='.\config.cfg'):
    sections = defaultdict(dict)

    if os.path.isfile(config_path):
        with open(config_path, 'r') as f:
            section_header=None

            for line in f.readlines():
                section = r'\[[\w\s*]+\]\n'
                if re.fullmatch(section, line):
                    section_header = line.replace('[', '').replace(']\n', '')
                    sections[section_header] = dict()
                elif '=' in line:
                    k, v = line.split('=')
                    sections[section_header][k.strip()] = v.strip()
    return sections


def write_config(config_path='.\config.cfg'):
    open_config = False
    required_sections = {
        'ODBC Drivers': {'ODBC_DRIVER': '', 'NATIVE_DRIVER': ''},
        'GDAL DATA': {'GDAL_DATA_LOC': ''},
        'DEFAULT DATABASE': {'type': '', 'server': '', 'database': ''}
    }
    existing_sections = read_config(config_path)

    for rec_section in required_sections.keys():
        if rec_section not in existing_sections.keys():
            if rec_section == 'ODBC Drivers':
                odbc = SqlDriver()
                existing_sections[rec_section]['ODBC_DRIVER']=odbc.odbc_driver
                existing_sections[rec_section]['NATIVE_DRIVER']=odbc.native_driver
            elif rec_section == 'GDAL DATA':
                existing_sections[rec_section]['GDAL_DATA']=get_gdal_data_path()
            else:
                print(f'\nMissing section {rec_section} from config file. Plesae edit {config_path} to add')
                open_config = True
                existing_sections[rec_section]=required_sections[rec_section]

    with open(config_path, 'w') as f:
        for section in existing_sections.keys():
            f.write(f'\n[{section}]\n')
            for k in existing_sections[section].keys():
                f.write(f'{k}={existing_sections[section][k]}\n')
    if open_config:
        os.startfile(config_path)
    return existing_sections



if __name__ == '__main__':
    sections= write_config(config_path='.\config.cfg')
