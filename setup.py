from distutils.core import setup

# import pkg_resources  # part of setuptools
# version = pkg_resources.require("ris")[0].version

setup(name='pysqldb3',
      version='0.0.2',
      packages=['pysqldb3'],
      description='Basic modules used by RIS',
      install_requires=[
          'psycopg2',
          'pymssql',
          'pandas',
          'requests',
          'xlrd',
          'xlwt',
          'openpyxl',
          'fuzzywuzzy',
          'tqdm',
          'configparser',
          'shapely'
      ]
      )

# to package run (setup.py sdist) from cmd
# to install unzip, and run (python setup.py install) from the cmd in the folder