import os
from spatialist.ancillary import finder
from .database import Database


def filewalker(directory, dbname='isos_db', user='user', password='password', port=8888, update=True):
    """
    gets dir, searches for s1 and s2, stores into tables ExistS1/2 with note of readability

    Parameters
    ----------
    directory: str
        path to data to be searched
    dbname: str
        name of database, default 'isos_db'
    user: str
    password: str
    port: int
    update: bool
        update the exists table, default true to be up to date

    Returns
    -------
    """
    pattern_s1 = '^S1[AB]_(S1|S2|S3|S4|S5|S6|IW|EW|WV|EN|N1|N2|N3|N4|N5|N6|IM)_(SLC|GRD|OCN)(F|H|M|_)_' \
                 '(1|2)(S|A)(SH|SV|DH|DV|VV|HH|HV|VH)_([0-9]{8}T[0-9]{6})_([0-9]{8}T[0-9]{6})_([0-9]{6})_' \
                 '([0-9A-F]{6})_([0-9A-F]{4}).zip$'
    pattern_s2 = '^S2[AB]_(MSIL1C|MSIL2A)_([0-9]{8}T[0-9]{6})_N([0-9]{4})_R([0-9]{3})_' \
                 'T([0-9A-Z]{5})_([0-9]{8}T[0-9]{6}).zip$'
    scenes_s1 = finder(directory, [pattern_s1], recursive=True, regex=True)
    scenes_s2 = finder(directory, [pattern_s2], recursive=True, regex=True)

    with Database(dbname, user=user, password=password, port=port) as db:
        orderly_exist_s1 = []
        for scene in scenes_s1:
            orderly_exist_s1.append({'scene': scene,
                                     'outname_base': os.path.basename(scene),
                                     'read_permission': int(os.access(scene, os.R_OK)),
                                     'file_size_MB': int(os.stat(scene).st_size / (1024 * 1024)),
                                     'owner': os.stat(scene).st_uid
                                     })
        orderly_exist_s2 = []
        for scene in scenes_s2:
            orderly_exist_s2.append({'scene': scene,
                                     'outname_base': os.path.basename(scene),
                                     'read_permission': int(os.access(scene, os.R_OK)),
                                     'file_size_MB': int(os.stat(scene).st_size / (1024 * 1024)),
                                     'owner': os.stat(scene).st_uid})

        db.insert(table='existings1', primary_key=db.get_primary_keys('existings1'),
                  orderly_data=orderly_exist_s1, update=update)
        db.insert(table='existings2', primary_key=db.get_primary_keys('existings2'),
                  orderly_data=orderly_exist_s2, update=update)


def ingest_from_exist_table(dbname='isos_db', user='user', password='password', port=8888, update=True):
    """
    gets data from exists tables with read permission and ingests the metadata into the according tables

    Parameters
    ----------
    dbname: str
        name of database, default 'isos_db'
    user: str
    password: str
    port: int
    update: bool
        update the exists table, default true to be up to date

    Returns
    -------
    """

    with Database(dbname, user=user, password=password, port=port) as db:
        session = db.Session()
        scene_dirs = session.query(db.load_table('existings1').c.scene).filter(
            db.load_table('existings1').c.read_permission == 1).all()
        ingest = []
        for i in scene_dirs:
            ingest.append(i[0])
        db.ingest_s1_from_id(ingest, update=update)
        scene_dirs = session.query(db.load_table('existings2').c.scene).filter(
            db.load_table('existings2').c.read_permission == 1).all()
        ingest = []
        for i in scene_dirs:
            ingest.append(i[0])
        db.ingest_s2_from_id(ingest, update=update)


def cronjob_task(directory, dbname, user, password, port, update=True):
    """
    function to run the periodic table update

    Parameters
    ----------
    directory: str
        path to data to be searched
    dbname: str
        name of database, default 'isos_db'
    user: str
    password: str
    port: int
    update: bool
        update the exists table, default true to be up to date

    Returns
    -------
    """
    filewalker(directory, dbname, user, password, port, update)
    ingest_from_exist_table(dbname, user, password, port, update)

