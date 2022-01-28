import os
from spatialist.ancillary import finder
from .database import Database


def filesweeper(directory, user, password, port):
    """
    gets dir, searches for s1 and s2, stores into tables ExistS1/2 with note of readability

    Parameters
    ----------
    directory: str
        path to data to be searched
    user: str
    password: str
    port: int
    Returns
    -------
    """

    # make list of dirs for input to exist tables
    scenes_s1 = finder(directory, ['^S1[AB]_.*.zip$'], recursive=True, regex=True)
    scenes_s2 = finder(directory, ['^S2[AB]_.*.zip$'], recursive=True, regex=True)
    print('searching done!')

    with Database('isos_db', user=user, password=password, port=port) as db:
        es1_colnames = {i.name: i.type for i in db.load_table('existings1').c}
        es2_colnames = {i.name: i.type for i in db.load_table('existings2').c}

        if str(es1_colnames) == str(es2_colnames) and \
                str(es2_colnames) == "{'scene': VARCHAR(), 'read_permission': INTEGER(), 'outname_base': VARCHAR()}":
            print('works')

        orderly_exist_s1 = []
        for scene in scenes_s1:
            orderly_exist_s1.append({'scene': scene,
                                     'read_permission': int(os.access(scene, os.W_OK)),
                                     'outname_base': os.path.basename(scene)})

        orderly_exist_s2 = []
        for scene in scenes_s2:
            orderly_exist_s2.append({'scene': scene,
                                     'read_permission': int(os.access(scene, os.W_OK)),
                                     'outname_base': os.path.basename(scene)})

        db.insert(table='existings1', primary_key=db.get_primary_keys('existings1'), orderly_data=orderly_exist_s1)
        db.insert(table='existings2', primary_key=db.get_primary_keys('existings2'), orderly_data=orderly_exist_s2)
