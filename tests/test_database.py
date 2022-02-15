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


def test_archive(tmpdir, testdata, testdir):
    pguser = os.environ.get('PGUSER')
    pgpassword = os.environ.get('PGPASSWORD')
    pgport = os.environ.get('PGPORT')
    if pgport is not None:
        pgport = int(pgport)
    else:
        pgport = 5432

    with isos.Database('test_isos', port=pgport, user='markuszehner', password=pgpassword) as db:
        isos.drop_archive(db)

    isos.filewalker.filesweeper(testdir, 'markuszehner', pgpassword, pgport)

    id = identify(testdata['s1'])
    with isos.Database('test_isos', port=pgport, user='markuszehner', password=pgpassword) as db:

        assert db._Database__is_open('localhost', 5432) is True  # checked in init
        assert db._Database__check_host('localhost', 5432) is True  # checked in init

        # assert db.load_table('duplicates') # how to check this?
        assert db.identify_sentinel2_from_folder([testdata['s2']])[0]['product_uri'] == \
               'S2B_MSIL2A_20220117T095239_N0301_R079_T32QMG_20220117T113605.SAFE'
        # __refactor_sentinel2data # checked in identify_sentinel2_from_folder
        assert isinstance(db.parse_id(testdata['s1']), list)

        # ingest data
        db.ingest_s2_from_id(testdata['s2'])
        db.ingest_s1_from_id(testdata['s1'])
        # check ingests
        assert db.is_registered(testdata['s2'], 'sentinel2data') is True
        assert db.is_registered(testdata['s1'], 'sentinel1data') is True
        # test rejecting doubles, and adding to duplicates
        db.ingest_s2_from_id(testdata['s2'])
        db.ingest_s1_from_id(testdata['s1'])

        db.drop_element(testdata['s1'], 'duplicates')

        db.add_tables(mytable)
        assert db._Database__check_table_exists('mytable') is True
        assert 'mytable' in db.get_tablenames(return_all=True)
        db.drop_table('mytable')
        assert db._Database__check_table_exists('mytable') is False
        assert db._Database__select_missing('duplicates') == []
        db.cleanup()

        assert db.get_primary_keys('sentinel2data') == ['scene']
        assert len(db.get_unique_directories('sentinel2data')) == 1

        assert len(db.filter_scenelist([testdata['s2'], testdata['s2_2']], 'sentinel2data')) == 1
        db.drop_element(testdata['s2'], 'sentinel2data')
        assert len(db.filter_scenelist([testdata['s2'], testdata['s2_2']], 'sentinel2data')) == 2

        assert db.get_colnames('duplicates') == ['outname_base', 'scene']

        assert db.size == (5, 1)  # 3 tables with combined 2 scenes ingested
        db.ingest_s2_from_id(testdata['s2'])

        assert db.query_db('sentinel2data', ['processing_level'], product_type='S2MSI2A') == \
               [{'processing_level': 'Level-2A'}]

        assert db.query_db('sentinel2data', ['product_type'], processing_level='Level-2A') == \
               [{'product_type': 'S2MSI2A'}]

        #print(db.query_db('sentinel1data', ['scene'], orbit='A'))
        assert db.query_db('sentinel1data', ['sensor', '"orbitNumber_rel"'], acquisition_mode='IW',
                          lines=16685, vv=1) == [{'orbitNumber_rel': 117, 'sensor': 'S1A'}]
        db.ingest_s2_from_id(testdata['s2_dup'])

        assert db.count_scenes('sentinel2data') == \
               [('S2B_MSIL2A_20220117T095239_N0301_R079_T32QMG_20220117T113605.zip', 2)]

        es1_colnames = {i.name: i.type for i in db.load_table('existings1').c}
        es2_colnames = {i.name: i.type for i in db.load_table('existings2').c}

        assert str(es1_colnames) == str(es2_colnames)
        assert str(es2_colnames) == "{'scene': VARCHAR(), 'read_permission': INTEGER(), 'outname_base': VARCHAR()}"
        #print('works')

    with isos.Database('test_isos', port=pgport, user='markuszehner', password=pgpassword) as db:
        isos.drop_archive(db)

    #     db.__prepare_update()
    #     db.insert  # checked in ingest_s2_from_id and ingest_s1_from_id
    #     db.cleanup
    #     db.export2shp
    #     move
    #     close
    #     __exit__
    #
    # tables_to_create # tested in init



