# drawn largely from the pyroSAR archive and rcm database, also ARD TDC
import importlib
import inspect
import subprocess
from datetime import datetime
from dateutil import parser
import gc
import os
import re
import shutil
import sys
import socket
import time
import logging
import progressbar as pb
from pathlib import Path
from osgeo import gdal

from spatialist import Vector
from pyroSAR.drivers import identify, ID

from sqlalchemy import create_engine, Table, MetaData, exists
from sqlalchemy import inspect as sql_inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.automap import automap_base
from sqlalchemy_utils import database_exists, create_database, drop_database
from geoalchemy2 import WKTElement

from .database_tables import *  # needs to stay here to create tables


log = logging.getLogger(__name__)


class Database(object):
    """
    Utility for storing image metadata in a database
    Parameters
    ----------
    dbname: str
        The name for the PostgreSQL database.
    user: str
        required for postgres driver: username to access the database. Default: 'postgres'
    password: str
        required for postgres driver: password to access the database. Default: '1234'
    host: str
        required for postgres driver: host where the database is hosted. Default: 'localhost'
    port: int
        required for postgres driver: port number to the database. Default: 5432
    cleanup: bool
        check whether all registered scenes exist and remove missing entries?
    """

    def __init__(self, dbname, user='user',
                 password='password', host='localhost', port=5432, cleanup=True):
        self.driver = 'postgresql'
        if not self.__check_host(host, port):
            sys.exit('Server not found!')

        # create dict, with which a URL to the db is created
        self.url_dict = {'drivername': self.driver,
                         'username': user,
                         'password': password,
                         'host': host,
                         'port': port,
                         'database': dbname}

        # create engine, containing URL and driver
        log.debug('starting DB engine for {}'.format(URL(**self.url_dict)))
        self.url = URL(**self.url_dict)
        self.engine = create_engine(self.url, echo=False)

        # if database is new, (create postgres-db and) enable spatial extension
        if not database_exists(self.engine.url):

            log.debug('creating new PostgreSQL database')
            create_database(self.engine.url)
            log.debug('enabling spatial extension for new database')
            self.conn = self.engine.connect()
            self.conn.execute('CREATE EXTENSION postgis;')
        else:
            self.conn = self.engine.connect()
        # create Session (ORM) and get metadata
        self.Session = sessionmaker(bind=self.engine)
        self.meta = MetaData(self.engine)
        self.add_tables(tables_to_create())
        # reflect tables from (by now) existing db, make some variables available within self
        self.Base = automap_base(metadata=self.meta)
        self.Base.prepare(self.engine, reflect=True)
        self.dbname = dbname

        if cleanup:
            log.info('checking for missing scenes')
            self.cleanup()
            sys.stdout.flush()

    # Table creation and addressing stuff
    def get_class_by_tablename(self, table):
        """Return class reference mapped to table.
        adapted from OrangeTux's comment on
        https://stackoverflow.com/questions/11668355/sqlalchemy-get-model-from-table-name-this-may-imply-appending-some-function-to
        Parameters
        ----------
        table: str
            String with name of table.
        Returns
        -------
        Class reference or None.
        """
        for c in self.Base.classes:
            if hasattr(c, '__table__') and str(c.__table__) == table:
                return c

    def load_table(self, table):
        """
        helper function
        load a table per `sqlalchemy.Table`

        Parameters
        ----------
        table: str
            name of table to be loaded

        Returns
        -------
        sqlalchemy.Table
        """
        return Table(table.lower(), self.meta, autoload=True, autoload_with=self.engine)

    def add_tables(self, tables):
        """
        Add tables to the database per :class:`sqlalchemy.schema.Table`
        Tables provided here will be added to the database.

        Parameters
        ----------
        tables: :class:`sqlalchemy.schema.Table` or :obj:`list` of :class:`sqlalchemy.schema.Table`
            The table(s) to be added to the database.
        """
        created = []
        if isinstance(tables, list):
            for table in tables:
                table.metadata = self.meta
                if not sql_inspect(self.engine).has_table(str(table)):
                    table.create(self.engine)
                    created.append(str(table))
        else:
            table = tables
            table.metadata = self.meta
            if not sql_inspect(self.engine).has_table(str(table)):
                table.create(self.engine)
                created.append(str(table))
        log.info('created table(s) {}.'.format(', '.join(created)))
        self.Base = automap_base(metadata=self.meta)
        self.Base.prepare(self.engine, reflect=True)

    def __check_table_exists(self, table):
        """
        returns true if table exists
        Parameters
        ----------
        table: str
            name of the table
        Returns
        -------
        bool
            does the table exist
        """
        tables = self.get_tablenames(return_all=True)
        if table not in tables:
            log.info('Table {} does not exist in the database {}'.format(table, self.dbname))
            return False
        return True

    # Insert data and preparation stuff
    def __prepare_update(self, table, primary_key, verbose=False, **args):
        """
        generic update string generator for tables
        Parameters
        ----------
        table: str
            table for which insertion string should be created
        primary_key: list of str
            primary key of table within list, or combined key as list of keys
        verbose: bool
            log additional info

        Returns
        -------
        insertion string
        """
        if self.__check_table_exists(table):
            col_names = self.get_colnames(table)
            arg_invalid = [x for x in args.keys() if x not in col_names]
            if len(arg_invalid) > 0:
                if verbose:
                    log.info('Following arguments {} were not ingested in table {}'.format(', '.join(arg_invalid), table))
            update = self.meta.tables[table].update()
            for p_key in primary_key:
                update = update.where(self.meta.tables[table].c[p_key] == args[p_key])
            update = update.values(**args)
            return update

    def __select_missing(self, table):
        """
        Parameters
        ----------
        table: str
            Table to query
        Returns
        -------
        list
            the names of all scenes, which are no longer stored in their registered location in requested table
        """
        table_schema = self.load_table(table)

        scenes = self.Session().query(table_schema.c.scene)
        files = [self.encode(x[0]) for x in scenes]
        return [x for x in files if not os.path.isfile(x)]

    def identify_sentinel2_from_folder(self, scene_dirs):
        """
        Method to open Sentinel-2 .zips with the vsizip in GDAL to read out metadata and ingest them in the
        table sentinel2data.

        Parameters
        ----------
        scene_dirs: str or list of str
            list of Sentinel-2 zip paths

        Returns
        -------
        list of dict
            orderly data from __refactor_sentinel2data
        """
        metadata = []
        tmp = os.environ.get('CPL_ZIP_ENCODING')
        os.environ['CPL_ZIP_ENCODING'] = 'UTF-8'
        if isinstance(scene_dirs, str):
            scene_dirs = [scene_dirs]

        for filename in scene_dirs:
            if filename.endswith('.incomplete'):
                continue
            name_dot_safe = Path(filename).stem + '.SAFE'
            xml_file = None
            if name_dot_safe[4:10] == 'MSIL2A':
                xml_file = gdal.Open(
                    '/vsizip/' + os.path.join(filename, name_dot_safe, 'MTD_MSIL2A.xml'))

            elif name_dot_safe[4:10] == 'MSIL1C':
                xml_file = gdal.Open(
                    '/vsizip/' + os.path.join(filename, name_dot_safe, 'MTD_MSIL1C.xml'))
            # this way we can open most raster formats and read metadata this way, just adjust the ifs..

            if xml_file:
                metadata.append([filename, xml_file])
                xml_file = None
        orderly_data = self.__refactor_sentinel2data(metadata)
        return orderly_data

    def parse_id(self, scenes):
        """
        Helper method to refactor Sentinel-1 id objects, make keys lower, replace ' ' by '_',
        make values the right unit types.
        ----------
        scenes: list of str
            s1 id objects
        Returns
        -------
        list of dict
            reformatted data
        """
        table_schema_cols = self.load_table('sentinel1data').c
        coltypes = {}
        for i in table_schema_cols:
            coltypes[i.name] = i.type

        orderly_data = []

        if not isinstance(scenes, list):
            scenes = [scenes]

        for scene in scenes:
            if isinstance(scene, ID):
                id = scene
            else:
                try:
                    id = identify(scene)
                except RuntimeError:
                    print(scene)
                    continue

            pols = [x.lower() for x in id.polarizations]

            temp_dict = {}
            for attribute in list(coltypes.keys()):
                if attribute == 'outname_base':
                    temp_dict[attribute] = id.outname_base()
                elif attribute in ['bbox', 'geometry']:
                    geom = getattr(id, attribute)()
                    geom.reproject(4326)
                    geom = geom.convert2wkt(set3D=False)[0]
                    temp_dict[attribute] = 'SRID=4326;' + str(geom)
                elif attribute in ['hh', 'vv', 'hv', 'vh']:
                    temp_dict[attribute] = int(attribute in pols)
                else:
                    if hasattr(id, attribute):
                        temp_dict[attribute] = getattr(id, attribute)
                    elif attribute in id.meta.keys():
                        temp_dict[attribute] = id.meta[attribute]
                    else:
                        raise AttributeError('could not find attribute {}'.format(attribute))
            orderly_data.append(temp_dict)
        return orderly_data

    def __refactor_sentinel2data(self, metadata_as_list_of_dicts):
        """
        Helper method to refactor Sentinel-2 metadata dicts, make keys lower, replace ' ' by '_',
        make values the right unit types. Add outname base from first field in list.
        Parameters
        ----------
        metadata_as_list_of_dicts: list of [str, dict]
            s2 metadata
        Returns
        -------
        list of dict
            reformatted data
        """
        # table_schema_cols = self.load_table('sentinel2data').c
        # coltypes = {}
        # for i in table_schema_cols:
        #     coltypes[i.name] = i.type
        coltypes = {i.name: i.type for i in self.load_table('sentinel2data').c}

        orderly_data = []
        for entry in metadata_as_list_of_dicts:
            temp_dict = {}
            for key, value in entry[1].GetMetadata().items():
                key = key.lower().replace(' ', '_')
                if str(coltypes.get(key)) == 'VARCHAR':
                    temp_dict[key] = value
                if str(coltypes.get(key)) == 'INTEGER':
                    temp_dict[key] = int(value.replace('.0', ''))
                if str(coltypes.get(key)) in ['DOUBLE PRECISION', 'FLOAT', 'DOUBLE_PRECISION']:
                    temp_dict[key] = float(value)
                if str(coltypes.get(key)) in ['TIMESTAMP', 'TIMESTAMP WITHOUT TIME ZONE', 'DATETIME']:
                    if value != '' and value:
                        try:
                            temp_dict[key] = parser.parse(value)  # , '%Y-%m-%dT%H:%M:%SZ')
                        except ValueError:
                            try:
                                temp_dict[key] = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ')
                            except ValueError:
                                print(entry)
                                print(coltypes)
                if str(coltypes.get(key)) in ['geometry(POLYGON,4326)']:
                    temp_dict[key] = WKTElement(value, srid=4326)

            temp_dict['outname_base'] = os.path.basename(entry[0])
            temp_dict['scene'] = entry[0]
            orderly_data.append(temp_dict)
        return orderly_data

    def ingest_s1_from_id(self, scene_dirs, update=False, verbose=False):

        orderly_data = self.parse_id(scene_dirs)

        self.insert(table='sentinel1data', primary_key=self.get_primary_keys('sentinel1data'),
                    orderly_data=orderly_data, verbose=verbose, update=update)

    def ingest_s2_from_id(self, scene_dirs, update=False, verbose=False):
        """
        ingest Sentinel-2 .zips into table sentinel2data.

        Parameters
        ----------
        scene_dirs: str or list of str
            list of Sentinel-2 zip paths
        update: bool
            update database? will update matching entries
        verbose: bool
            log additional info

        Returns
        -------
        """
        orderly_data = self.identify_sentinel2_from_folder(scene_dirs)

        self.insert(table='sentinel2data', primary_key=self.get_primary_keys('sentinel2data'),
                    orderly_data=orderly_data, verbose=verbose, update=update)

    def insert(self, table, primary_key, orderly_data, verbose=False, update=False):
        """
        Generic insert for tables, checks if entry is already in db,
        update can be used to overwrite all concerning entries
        Parameters
        ----------
        table: str
            table in which data insertion should be
        primary_key: list of str
            primary key of table within list, or combined key as list of keys
        orderly_data: list of dicts
            list of dicts created by xx_01.loadwd.make_a_list
        verbose: bool
            log additional info
        update: bool
            update database? will update all entries given in orderly_data


        Returns
        -------

        """
        if len(orderly_data) == 0:
            log.info(f'no scenes found for table {table}!')
            return

        self.Base = automap_base(metadata=self.meta)
        self.Base.prepare(self.engine, reflect=True)

        self.__check_table_exists(table)
        table_schema = self.load_table(table)
        col_names = self.get_colnames(table)

        reduce_entry = True if not set(col_names) == set(orderly_data[0].keys()) else False

        session = self.Session()
        rejected = []

        tableobj = self.Base.classes[table]

        for entry in orderly_data:
            if reduce_entry:
                entry = {key: entry[key] for key in col_names if key in entry}

            exists_str = exists()
            for p_key in primary_key:
                exists_str = exists_str.where(table_schema.c[p_key] == entry[p_key])
            ret = session.query(exists_str).scalar()

            if ret:
                if update:
                    self.conn.execute(self.__prepare_update(table, primary_key, **entry))
                rejected.append(entry)
            else:
                session.add(tableobj(**entry))
                session.commit()
        session.close()
        message = 'Ingested {} entries to table {}'.format(len(orderly_data) - len(rejected), table)
        if len(rejected) > 0:
            if verbose:
                if update:
                    log.info('Updated entries with already existing primary key: ', rejected)
                else:
                    log.info('Rejected entries with already existing primary key: ', rejected)
            if update:
                message += ', updated {} (already existing).'.format(len(rejected))
            else:
                message += ', rejected {} (already existing).'.format(len(rejected))
        log.info(message)
        session.close()

    def is_registered(self, scene, table):
        """
        Simple check if a scene is already registered in the database.
        Parameters
        ----------
        scene: str or ID
            the SAR scene
        table:
            from which table to check
        Returns
        -------
        bool
            is the scene already registered?
        """
        try:
            # not nice, try to determine s1 or s2 by name!
            id = scene if isinstance(scene, ID) else identify(scene)
            id = self.parse_id(id)[0]
        except RuntimeError:
            id = self.identify_sentinel2_from_folder(scene)[0]

        self.__check_table_exists(table)
        table_schema = self.load_table(table)
        session = self.Session()

        primary_key = self.get_primary_keys(table)

        exists_str = exists()
        for p_key in primary_key:
            exists_str = exists_str.where(table_schema.c[p_key] == id[p_key])
        ret = session.query(exists_str).scalar()
        session.close()

        if ret:
            return True
        return False

    def cleanup(self):
        """
        Remove all scenes from the database, which are no longer stored in their registered location
        Returns
        -------
        """
        tables = self.get_tablenames()
        for table in tables:
            missing = self.__select_missing(table)
            for scene in missing:
                log.info('Removing missing scene from database tables: {}'.format(scene))
                self.drop_element(scene, table)

    # misc methods
    @staticmethod
    def encode(string, encoding='utf-8'):
        if not isinstance(string, str):
            return string.encode(encoding)
        else:
            return string

    def export2shp(self, path, table):
        """
        export the database to a shapefile
        Parameters
        ----------
        path: str
            the path of the shapefile to be written.
            This will overwrite other files with the same name.
            If a folder is given in path it is created if not existing.
            If the file extension is missing '.shp' is added.
        table: str
            the table to write to the shapefile

        Returns
        -------
        """
        if table not in self.get_tablenames():
            log.warning('table not in database!')
            return

        # add the .shp extension if missing
        if not path.endswith('.shp'):
            path += '.shp'

        # creates folder if not present, adds .shp if not within the path
        dirname = os.path.dirname(path)
        os.makedirs(dirname, exist_ok=True)

        db_connection = """PG:host={0} port={1} user={2}
            dbname={3} password={4} active_schema=public""".format(self.url_dict['host'],
                                                                   self.url_dict['port'],
                                                                   self.url_dict['username'],
                                                                   self.url_dict['database'],
                                                                   self.url_dict['password'])
        subprocess.call(['ogr2ogr', '-f', 'ESRI Shapefile', path,
                         db_connection, table])

    # utilities
    def filter_scenelist(self, scenelist, table):
        """
        Filter a list of scenes by file names already registered in the database.
        Parameters
        ----------
        scenelist: :obj:`list` of :obj:`str` or :obj:`pyroSAR.drivers.ID`
            the scenes to be filtered
        table: str
            table to be searched
        Returns
        -------
        list
            the file names of the scenes whose basename is not yet registered in the database
        """
        for item in scenelist:
            if not isinstance(item, (ID, str)):
                raise TypeError("items in scenelist must be of type 'str' or 'pyroSAR.ID'")

        table_schema = self.load_table(table)
        # ORM query, get all scenes locations
        scenes_data = self.Session().query(table_schema.c.scene)
        registered = [os.path.basename(self.encode(x[0])) for x in scenes_data]
        names = [item.scene if isinstance(item, ID) else item for item in scenelist]
        filtered = [x for x, y in zip(scenelist, names) if os.path.basename(y) not in registered]
        return filtered

    def get_colnames(self, table):
        """
        Return the names of all columns of a table.
        Parameters
        ----------
        table: str
            tablename
        Returns
        -------
        list
            the column names of the chosen table
        """
        dicts = sql_inspect(self.engine).get_columns(table)
        col_names = [i['name'] for i in dicts]

        return sorted([self.encode(x) for x in col_names])

    def get_tablenames(self, return_all=False):
        """
        Return the names of all tables in the database

        Parameters
        ----------
        return_all: bool
            only gives tables data and duplicates on default.
            Set to True to get all other tables and views created automatically.
        Returns
        -------
        list
            the table names
        """
        #  the method was intended to only return user generated tables by default, as well as data and duplicates
        all_tables = ['spatial_ref_sys']
        # get tablenames from metadata
        insp = sql_inspect(self.engine)
        tables = sorted([self.encode(x) for x in insp.get_table_names()])
        if return_all:
            return tables
        else:
            ret = []
            for i in tables:
                if i not in all_tables:
                    ret.append(i)
            return ret

    def get_primary_keys(self, table):
        """
        retrieve primary keys of a table

        Parameters
        ----------
        table: str
            table name
        Returns
        -------
        list
            primary keys of table
        """
        return [key.name for key in self.load_table(table).primary_key]

    def get_unique_directories(self, table):
        """
        Get a list of directories containing registered scenes

        Parameters
        ----------
        table: str
            table name
        Returns
        -------
        list
            the directory names
        """
        # ORM query, get all directories
        table = self.get_class_by_tablename(table)
        scenes = self.Session().query(table.scene)
        registered = [os.path.dirname(self.encode(x[0])) for x in scenes]
        return list(set(registered))

    def move(self, table, scenelist, directory, pbar=False):
        """
        Move a list of files while keeping the database entries up to date.
        If a scene is registered in the database (in either the data or duplicates table),
        the scene entry is directly changed to the new location.
        Parameters
        ----------
        table: str
            table to move scenes from
        scenelist: list
            the file locations
        directory: str
            a folder to which the files are moved
        pbar: bool
            show a progress bar?
        Returns
        -------
        """
        if not os.path.isdir(directory):
            os.mkdir(directory)
        if not os.access(directory, os.W_OK):
            raise RuntimeError('directory cannot be written to')
        failed = []
        double = []
        if pbar:
            progress = pb.ProgressBar(max_value=len(scenelist)).start()
        else:
            progress = None

        for i, scene in enumerate(scenelist):
            new = os.path.join(directory, os.path.basename(scene))
            if os.path.isfile(new):
                double.append(new)
                continue
            try:
                shutil.move(scene, directory)
            except shutil.Error:
                failed.append(scene)
                continue
            finally:
                if progress is not None:
                    progress.update(i + 1)
            if self.query_db(table=table, scene=scene) != 0:
                table_name = table
            else:
                table_name = None
            if table_name:
                # using core connection to execute SQL syntax (as was before)
                self.conn.execute('''UPDATE {0} SET scene= '{1}' WHERE scene='{2}' '''.format(table_name, new, scene))
        if progress is not None:
            progress.finish()

        if len(failed) > 0:
            log.info('The following scenes could not be moved:\n{}'.format('\n'.join(failed)))
        if len(double) > 0:
            log.info('The following scenes already exist at the target location:\n{}'.format('\n'.join(double)))

    def query_db(self, table, selected_columns='*', vectorobject=None, date=None, verbose=False, **args):
        """
        select from the database, bases on pyrosar.Archive.select
        todo make this working reliably maybe with select from sqlalchemy
        Parameters
        ----------
        table: str
            specify from which table to select. get available names per :meth:`~RCMArchive.archive.get_tablenames()`
        selected_columns: list or str
            list of columns which should be returned by the query, default is all columns
        vectorobject: :class:`~spatialist.vector.Vector`
            a geometry with which the scenes need to overlap
        date: str or list
            either one date or a range from - to in a list
        verbose: bool
            log additional info
        **args:
            any further arguments (columns), which are registered in the database.
            See :meth:`~RCMArchive.archive.get_colnames()`
        Returns
        -------
        list
            the entries returned by the selection
        """
        # check if table exists
        if not self.__check_table_exists(table):
            return []

        # check if table is empty
        if len([{column: value for column, value in rowproxy.items()}
                for rowproxy in self.conn.execute('SELECT True FROM {} LIMIT 1;'.format(table))]) == 0:
            log.info('Table {} is empty!'.format(table))
            return []
        col_names = self.get_colnames(table)

        # here the geometry_columns table is queried, it has info about all tables' geometry columns
        geom_list = [{column: value for column, value in rowproxy.items()}
                     for rowproxy in self.conn.execute('SELECT f_table_name, f_geometry_column '
                                                       'FROM geometry_columns;')]
        geom_col_name = None
        for entry in geom_list:
            if table in entry.values():
                geom_col_name = entry['f_geometry_column']

        if isinstance(date, list):  # TODO: see how to query datetime. check if date exists
            pass
            # mindate = date[0]
            # maxdate = date[1]

        arg_valid = [x for x in args.keys() if x in col_names]
        arg_invalid = [x for x in args.keys() if x not in col_names]
        if len(arg_invalid) > 0:
            log.info('the following arguments will be ignored as they are not registered in the data base: {}'.format(
                     ', '.join(arg_invalid)))

        if selected_columns != '*':
            if isinstance(selected_columns, str):
                selected_columns = [selected_columns]
            sel_col_invalid = [x for x in selected_columns if x not in col_names]
            if len(sel_col_invalid) > 0:
                log.info('the following selected columns will be ignored '
                         'as they are not registered in the table: {}'.format(', '.join(sel_col_invalid)))
        arg_format = []
        vals = []

        for key in arg_valid:
            if key == 'scene':
                arg_format.append('''scene LIKE '%%{0}%%' '''.format(os.path.basename(args[key])))
            else:
                if isinstance(args[key], (float, int, str)):
                    arg_format.append('''{0}='{1}' '''.format(key, args[key]))
                elif isinstance(args[key], (tuple, list)):
                    arg_format.append('''{0} IN ('{1}')'''.format(key, "', '".join(map(str, args[key]))))
            if key == 'mindate':
                if re.search('[0-9]{8}T[0-9]{6}', args[key]):
                    arg_format.append('start>=?')
                    vals.append(args[key])
                else:
                    log.info('WARNING: argument mindate is ignored, must be in format YYYYmmddTHHMMSS')
            if key == 'maxdate':
                if re.search('[0-9]{8}T[0-9]{6}', args[key]):
                    arg_format.append('stop<=?')
                    vals.append(args[key])
                else:
                    log.info('WARNING: argument maxdate is ignored, must be in format YYYYmmddTHHMMSS')

        if vectorobject and geom_col_name:
            if isinstance(vectorobject, Vector):
                vectorobject.reproject('+proj=longlat +datum=WGS84 +no_defs ')
                site_geom = vectorobject.convert2wkt(set3D=False)[0]

                arg_format.append("st_intersects({0}, 'SRID=4326; {1}')".format(
                    geom_col_name,
                    site_geom
                ))
            else:
                log.info('WARNING: argument vectorobject is ignored, must be of type spatialist.vector.Vector. '
                      'Check also if table has geom column!')

        query = '''SELECT {} FROM {}'''.format(', '.join(selected_columns), table)
        if len(arg_format) > 0:
            query += ''' WHERE {}'''.format(' AND '.join(arg_format))
        # the query gets assembled stepwise here
        for val in vals:
            query = query.replace('?', ''' '{0}' ''', 1).format(val)
        if verbose:
            log.info(query)
        # core SQL execution
        query_rs = self.conn.execute(query)
        return [{column: value for column, value in rowproxy.items()} for rowproxy in query_rs]

    @property
    def size(self):
        """
        get the number of scenes registered in the database
        Returns
        -------
        tuple
            the number of tables and scenes
        """
        # ORM query
        session = self.Session()
        tables = self.get_tablenames()

        num = 0
        for table in tables:
            table_schema = self.load_table(table)
            r1 = session.query(table_schema.c.scene).count()
            num += r1
        session.close()
        return len(tables), num

    def count_scenes(self, table):
        """
        returns basename and count of ingested scenes from the requested table

        Parameters
        ----------
        table: str
            table name
        Returns
        -------
        """
        if not self.__check_table_exists(table):
            log.info(f'table {table} not in database')
        else:
            table_schema = self.load_table(table)
            session = self.Session()
            ret = session.query(table_schema.c.outname_base, func.count(table_schema.c.outname_base)).\
                group_by(table_schema.c.outname_base).all()
            session.close()
            return ret

    def count_permission_state(self, table):
        """
        returns nr of readable files from the requested table

        Parameters
        ----------
        table: str
            table name
        Returns
        -------
        """
        ret = self.Session().query(func.sum(self.load_table(table).c.read_permission))
        return ret.scalar()

    # Database utilities
    def __enter__(self):
        return self

    def close(self):
        """
        close the database connection
        """
        self.Session().close()
        self.conn.close()
        self.engine.dispose()
        gc.collect(generation=2)  # this was added as a fix for win PermissionError when deleting sqlite.db files.

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def drop_element(self, scene, table):
        """
        Drop a scene from the data table.
        If duplicates table contains matching entry, it will be moved to the data table.
        Parameters
        ----------
        scene: str
            path of scene
        table: str
            name of table to drop element from
        Returns
        -------
        """
        # save outname_base from to be deleted entry
        table_schema = self.load_table(table)

        # delete entry in data table
        delete_statement = table_schema.delete().where(table_schema.c.scene == scene)
        self.conn.execute(delete_statement)

        log.info('Entry with scene-id: \n{} \nwas dropped from data!'.format(scene))

    def drop_table(self, table):
        """
        Drop a table from the database.
        Parameters
        ----------
        table: str
            tablename
        Returns
        -------
        """
        if table in self.get_tablenames(return_all=True):
            # this removes the idx tables and entries in geometry_columns for sqlite databases
            table_schema = self.load_table(table)

            table_schema.drop(self.engine)
            # table_new = self.meta.tables.get(table)
            # self.Base.metadata.drop_all(bind=self.engine, tables=[table_new])
            log.info('table {} dropped from database.'.format(table_schema))
        else:
            raise ValueError("table {} is not registered in the database!".format(table))
        self.meta = MetaData(self.engine)
        self.Base = automap_base(metadata=self.meta)
        self.Base.prepare(self.engine, reflect=True)

    @staticmethod
    def __is_open(ip, port):
        """
        Checks server connection, from Ben Curtis (github: Fmstrat)
        Parameters
        ----------
        ip: str
            ip of the server
        port: str
            port of the server
        Returns
        -------
        bool:
            is the server reachable?

        """
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(3)
        try:
            s.connect((ip, int(port)))
            s.shutdown(socket.SHUT_RDWR)
            return True
        except:
            return False
        finally:
            s.close()

    def __check_host(self, ip, port):
        """
        Calls __is_open() on ip and port, from Ben Curtis (github: Fmstrat)
        Parameters
        ----------
        ip: str
            ip of the server
        port: str or int
            port of the server
        Returns
        -------
        bool:
            is the server reachable?
        """
        ipup = False
        for i in range(2):
            if self.__is_open(ip, port):
                ipup = True
                break
            else:
                time.sleep(5)
        return ipup


def drop_archive(database):
    """
    drop (delete) a scene database

    Parameters
    ----------
    database: isos.database.Database
        the database to be deleted
    Returns
    -------

    See Also
    --------
    :func:`sqlalchemy_utils.functions.drop_database()`

    Examples
    --------
    >>> pguser = os.environ.get('PGUSER')
    >>> pgpassword = os.environ.get('PGPASSWORD')

    >>> db = Database('test', postgres=True, port=5432, user=pguser, password=pgpassword)
    >>> drop_archive(db)
    """
    url = database.url
    database.close()
    drop_database(url)


def tables_to_create(s1=True, s2=True):
    """
    Dynamically retrieve all table classes from database_tables
    Parameters
    ----------
    Returns
    -------
    list
        list of names of table classes
    """
    tables = []
    for name, cls in inspect.getmembers(importlib.import_module('isos.database_tables'), inspect.isclass):
        if cls.__module__ == 'isos.database_tables':
            if not s1 and name == 'Sentinel1Data':
                continue
            if not s2 and name in ['Sentinel2Data', 'Sentinel2Meta']:
                continue
            tables.append(eval(name).__table__)
    if len(tables) == 0:
        log.info('ERROR, no tables found to create!')

    return tables
