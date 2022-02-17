import os
from spatialist.ancillary import finder
from .database import Database


def filesweeper(directory, dbname='isos_db', user='user', password='password', port=8888, update=True):
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

    # make list of dirs for input to exist tables
    scenes_s1 = finder(directory, ['^S1[AB]_.*.zip$'], recursive=True, regex=True)
    scenes_s2 = finder(directory, ['^S2[AB]_.*.zip$'], recursive=True, regex=True)
    #print('searching done!')

    with Database(dbname, user=user, password=password, port=port) as db:
        es1_colnames = {i.name: i.type for i in db.load_table('existings1').c}
        es2_colnames = {i.name: i.type for i in db.load_table('existings2').c}

        # if not (str(es1_colnames) == str(es2_colnames) and
        #         str(es2_colnames) == "{'scene': VARCHAR(), 'outname_base': VARCHAR(), 'read_permission': INTEGER(),"
        #                              " 'file_size_MB': INTEGER(), 'owner': VARCHAR()}"):
            #print('Exists tables have changed!')

        orderly_exist_s1 = []
        for scene in scenes_s1:
            orderly_exist_s1.append({'scene': scene,
                                     'outname_base': os.path.basename(scene),
                                     'read_permission': int(os.access(scene, os.R_OK)),
                                     'file_size_MB': int(os.stat(scene).st_size / (1024 * 1024)),
                                     'owner': os.stat(scene).st_uid
                                     })
        #print(orderly_exist_s1)
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
    filesweeper(directory, dbname, user, password, port, update)
    ingest_from_exist_table(dbname, user, password, port, update)

