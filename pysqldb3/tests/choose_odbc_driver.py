import pyodbc
import re
import os


def choose_max(driver_list):
    """
    Ugly but functional
    todo:  rewrite
    :param driver_list:
    :return:
    """
    x = r'(?!RDA)\s+[0-9]+\s*'
    mx = 0

    numbers = [re.findall(x, _) for _ in driver_list]

    for _ in numbers:
        if int(_[0])>mx:
            mx=int(_[0])

    return [i for i in driver_list if str(mx) in i][0]



def get_drivers():
    odbc_drivers = list()
    native_drivers = list()

    for i in pyodbc.drivers():
        if 'ODBC Driver' in i:
            odbc_drivers.append(i)
        elif 'SQL Server Native Client' in i:
            native_drivers.append(i)
    return odbc_drivers, native_drivers


def get_latest_drivers():
    odbc_drivers, native_drivers = get_drivers()
    odbc_driver = choose_max(odbc_drivers)
    native_driver = choose_max(native_drivers)
    return odbc_driver, native_driver

def set_up_odbc():
    odbc_driver, native_driver = get_latest_drivers()
    if not os.path.isfile('config.cfg'):
        print ('Writing config...')
        with open('config.cfg', 'w') as f:
            f.write('[ODBC Drivers]\n')
            f.write(f'odbc_driver={odbc_driver}\n')
            f.write(f'native_driver={native_driver}\n')


if __name__ == '__main__':
    set_up_odbc()