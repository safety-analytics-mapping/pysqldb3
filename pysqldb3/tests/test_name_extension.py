from .. import pysqldb3 as pysqldb


def test_name_extension():
    name = 'sample_shapefile.shp'
    assert name == pysqldb.Shapefile.name_extension(name)

    name = 'sample_shapefile'
    assert name + '.shp' == pysqldb.Shapefile.name_extension(name)