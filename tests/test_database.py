import isos

import pytest
import platform
import tarfile as tf
import os
from datetime import datetime
from spatialist import Vector
from sqlalchemy import Table, MetaData, Column, Integer, String
from geoalchemy2 import Geometry
from pyroSAR import identify

metadata = MetaData()

mytable = Table('mytable', metadata,
                Column('mytable_id', Integer, primary_key=True),
                Column('value', String(50)),
                Column('shape', Geometry('POLYGON', management=True, srid=4326)))


def test_archive(tmpdir, testdata):
    pguser = os.environ.get('PGUSER')
    pgpassword = os.environ.get('PGPASSWORD')
    pgport = os.environ.get('PGPORT')
    if pgport is not None:
        pgport = int(pgport)
    else:
        pgport = 5432

    id = identify(testdata['s1'])
    db = isos.Database('test_isos_2', port=pgport, user='markuszehner', password=pgpassword)
    db.ingest_s2_from_id(testdata['s2'])
    assert db.is_registered(testdata['s2'], 'sentinel2data') is True

    # assert all(isinstance(x, str) for x in db.get_tablenames())
    # assert all(isinstance(x, str) for x in db.get_colnames())
    # assert db.is_registered(testdata['s1']) is True
    # assert len(db.get_unique_directories()) == 1
    # assert db.select_duplicates(outname_base='S1A__IW___A_20150222T170750', scene='scene.zip') == []
    # assert len(db.select(mindate='20141001T192312', maxdate='20201001T192312')) == 1
    # assert len(db.select(polarizations=['VV'])) == 1
    # assert len(db.select(vectorobject=id.bbox())) == 1
    # assert len(db.select(sensor='S1A', vectorobject='foo', processdir=str(tmpdir))) == 1
    # assert len(db.select(sensor='S1A', mindate='foo', maxdate='bar', foobar='foobar')) == 1
    # out = db.select(vv=1, acquisition_mode=('IW', 'EW'))
    # assert len(out) == 1
    # assert isinstance(out[0], str)
    #
    # db.insert(testdata['s1_3'])
    # db.insert(testdata['s1_4'])
    # db.drop_element(testdata['s1_3'])
    # assert db.size == (2, 0)
    # db.drop_element(testdata['s1_4'])
    #
    # db.add_tables(mytable)
    # assert 'mytable' in db.get_tablenames()
    # with pytest.raises(TypeError):
    #     db.filter_scenelist([1])
    db.close()
    #isos.drop_archive(db)

