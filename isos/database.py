# drawn largely from the pyroSAR archive and rcm database, also ARD TDC
import csv
import importlib
import inspect
import subprocess
from datetime import datetime
import gc
import os
import re
import shutil
import sys

import progressbar as pb  # use tqdm?

from spatialist import crsConvert, sqlite3, Vector, bbox
from spatialist.ancillary import parse_literal, finder

from sqlalchemy import create_engine, Table, MetaData, exists, and_, Column, Integer, String, exc
from sqlalchemy import inspect as sql_inspect
from sqlalchemy.event import listen
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select, func
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.automap import automap_base
from sqlalchemy_utils import database_exists, create_database, drop_database
from geoalchemy2 import WKTElement

from pyroSAR.drivers import identify, identify_many, ID
from spatialist.ancillary import finder
from .database_tables import *  # needs to stay here to create tables

from pathlib import Path
from osgeo import gdal

import socket
import time
import platform

import logging

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

    def __init__(self, dbname, user='postgres',
                 password='1234', host='localhost', port=5432, cleanup=True):
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

    def add_tables(self, tables):
        """
        Add tables to the database per :class:`sqlalchemy.schema.Table`
        Tables provided here will be added to the database.

        .. note::

            Columns using Geometry must have setting management=True for SQLite,
            for example: ``bbox = Column(Geometry('POLYGON', management=True, srid=4326))``

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

    def load_table(self, tablename):
        """
        load a table per `sqlalchemy.Table`
        """
        return Table(tablename.lower(), self.meta, autoload=True, autoload_with=self.engine)

    # def __prepare_insertion(self, scene):
    #     """
    #     read scene metadata and parse a string for inserting it into the database
    #     Parameters
    #     ----------
    #     scene: str or ID
    #         a SAR scene
    #     Returns
    #     -------
    #     object of class Data, insert string
    #     """
    #     id = scene if isinstance(scene, ID) else identify(scene)
    #     pols = [x.lower() for x in id.polarizations]
    #     # insertion as an object of Class Data (reflected in the init())
    #     insertion = self.Data()
    #     colnames = self.get_colnames()
    #     for attribute in colnames:
    #         if attribute == 'bbox':
    #             geom = id.bbox()
    #             geom.reproject(4326)
    #             geom = geom.convert2wkt(set3D=False)[0]
    #             geom = 'SRID=4326;' + str(geom)
    #             # set attributes of the Data object according to input
    #             setattr(insertion, 'bbox', geom)
    #         elif attribute in ['hh', 'vv', 'hv', 'vh']:
    #             setattr(insertion, attribute, int(attribute in pols))
    #         else:
    #             if hasattr(id, attribute):
    #                 attr = getattr(id, attribute)
    #             elif attribute in id.meta.keys():
    #                 attr = id.meta[attribute]
    #             else:
    #                 raise AttributeError('could not find attribute {}'.format(attribute))
    #             value = attr() if inspect.ismethod(attr) else attr
    #             setattr(insertion, str(attribute), value)
    #
    #     return insertion  # return the Data object

    def __select_missing(self, table):
        """
        Returns
        -------
        list
            the names of all scenes, which are no longer stored in their registered location in requested table
        """
        scenes = []

        table = self.load_table(table)

        scenes = self.Session().query(table.c.scene)
        files = [self.encode(x[0]) for x in scenes]
        return [x for x in files if not os.path.isfile(x)]

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
            print additional info

        Returns
        -------
        insertion string
        """
        if self.__check_table_exists(table):
            col_names = self.get_colnames(table)
            arg_invalid = [x for x in args.keys() if x not in col_names]
            if len(arg_invalid) > 0:
                if verbose:
                    print('Following arguments {} were not ingested in table {}'.format(', '.join(arg_invalid), table))
            update = self.meta.tables[table].update()
            for p_key in primary_key:
                update = update.where(self.meta.tables[table].c[p_key] == args[p_key])
            update = update.values(**args)
            return update

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
            print('Table {} does not exist in the database {}'.format(table, self.dbname))
            return False
        return True

    def insert(self, table, primary_key, orderly_data, verbose=False, update=False):
        """
        generic inserter for tables, checks if entry is already in db,
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
            print additional info
        update: bool
            update database? will overwrite all entries given in orderly_data

        Returns
        -------

        """
        self.Base = automap_base(metadata=self.meta)
        self.Base.prepare(self.engine, reflect=True)

        self.__check_table_exists(table)
        table_info = Table(table, self.meta, autoload=True, autoload_with=self.engine)
        session = self.Session()
        rejected = []

        # session could be used here maybe..
        # session = self.archive.Session()
        tableobj = self.Base.classes[table]

        for entry in orderly_data:
            if update:
                self.conn.execute(self.__prepare_update(table, primary_key, **entry))
            else:
                exists_str = exists()
                for p_key in primary_key:
                    exists_str = exists_str.where(table_info.c[p_key] == entry[p_key])
                ret = session.query(exists_str).scalar()

                if ret:
                    rejected.append(entry)
                else:
                    # self.archive.conn.execute(self.__prepare_insertion(table, **entry))
                    session.add(tableobj(**entry))
                    session.commit()
        session.close()
        message = 'Ingested {} entries to table {}'.format(len(orderly_data) - len(rejected), table)
        if len(rejected) > 0:
            if verbose:
                print('Rejected entries with already existing primary key: ', rejected)
            message += ', rejected {} (already existing).'.format(len(rejected))
        print(message)
        session.close()

    # def insert(self, scene_in, pbar=False, test=False):
    #     """
    #     Insert one or many scenes into the database
    #     Parameters
    #     ----------
    #     scene_in: str or ID or list
    #         a SAR scene or a list of scenes to be inserted
    #     pbar: bool
    #         show a progress bar?
    #     test: bool
    #         should the insertion only be tested or directly be committed to the database?
    #     """
    #     length = len(scene_in) if isinstance(scene_in, list) else 1
    #
    #     if isinstance(scene_in, (ID, str)):
    #         scene_in = [scene_in]
    #     if not isinstance(scene_in, list):
    #         raise RuntimeError('scene_in must either be a string pointing to a file, a pyroSAR.ID object '
    #                            'or a list containing several of either')
    #
    #     log.info('filtering scenes by name')
    #     scenes = self.filter_scenelist(scene_in)
    #     if len(scenes) == 0:
    #         log.info('...nothing to be done')
    #         return
    #     log.info('identifying scenes and extracting metadata')
    #     scenes = identify_many(scenes, pbar=pbar)
    #
    #     if len(scenes) == 0:
    #         log.info('all scenes are already registered')
    #         return
    #
    #     counter_regulars = 0
    #     counter_duplicates = 0
    #     list_duplicates = []
    #
    #     message = 'inserting {0} scene{1} into database'
    #     log.info(message.format(len(scenes), '' if len(scenes) == 1 else 's'))
    #     log.debug('testing changes in temporary database')
    #     if pbar:
    #         progress = pb.ProgressBar(max_value=len(scenes))
    #     else:
    #         progress = None
    #     basenames = []
    #     insertions = []
    #     session = self.Session()
    #     for i, id in enumerate(scenes):
    #         basename = id.outname_base()
    #         if not self.is_registered(id) and basename not in basenames:
    #             insertion = self.__prepare_insertion(id)
    #             insertions.append(insertion)
    #             counter_regulars += 1
    #             log.debug('regular:   {}'.format(id.scene))
    #         elif not self.__is_registered_in_duplicates(id):
    #             insertion = self.Duplicates(outname_base=basename, scene=id.scene)
    #             insertions.append(insertion)
    #             counter_duplicates += 1
    #             log.debug('duplicate: {}'.format(id.scene))
    #         else:
    #             list_duplicates.append(id.outname_base())
    #
    #         if progress is not None:
    #             progress.update(i + 1)
    #         basenames.append(basename)
    #
    #     if progress is not None:
    #         progress.finish()
    #
    #     session.add_all(insertions)
    #
    #     if not test:
    #         log.debug('committing transactions to permanent database')
    #         # commit changes of the session
    #         session.commit()
    #     else:
    #         log.info('rolling back temporary database changes')
    #         # roll back changes of the session
    #         session.rollback()
    #
    #     message = '{0} scene{1} registered regularly'
    #     log.info(message.format(counter_regulars, '' if counter_regulars == 1 else 's'))
    #     message = '{0} duplicate{1} registered'
    #     log.info(message.format(counter_duplicates, '' if counter_duplicates == 1 else 's'))

    def is_registered(self, scene):
        """
        Simple check if a scene is already registered in the database.
        Parameters
        ----------
        scene: str or ID
            the SAR scene
        Returns
        -------
        bool
            is the scene already registered?
        """
        id = scene if isinstance(scene, ID) else identify(scene)
        # ORM query, where scene equals id.scene, return first
        exists_data = self.Session().query(self.Data.outname_base).filter(
            self.Data.outname_base == id.outname_base()).first()
        exists_duplicates = self.Session().query(self.Duplicates.outname_base).filter(
            self.Duplicates.outname_base == id.outname_base()).first()
        in_data = False
        in_dup = False
        if exists_data:
            in_data = len(exists_data) != 0
        if exists_duplicates:
            in_dup = len(exists_duplicates) != 0
        return in_data or in_dup

    def __is_registered_in_duplicates(self, scene):
        """
        Simple check if a scene is already registered in the database.
        Parameters
        ----------
        scene: str or ID
            the SAR scene
        Returns
        -------
        bool
            is the scene already registered?
        """
        id = scene if isinstance(scene, ID) else identify(scene)
        # ORM query as in is registered
        exists_duplicates = self.Session().query(self.Duplicates.outname_base).filter(
            self.Duplicates.outname_base == id.outname_base()).first()
        in_dup = False
        if exists_duplicates:
            in_dup = len(exists_duplicates) != 0
        return in_dup

    def cleanup(self):
        """
        Remove all scenes from the database, which are no longer stored in their registered location
        Returns
        -------
        """
        missing = self.__select_missing('data')
        for scene in missing:
            log.info('Removing missing scene from database tables: {}'.format(scene))
            self.drop_element(scene, with_duplicates=True)

    @staticmethod
    def encode(string, encoding='utf-8'):
        if not isinstance(string, str):
            return string.encode(encoding)
        else:
            return string

    def export2shp(self, path, table='data'):
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
            the table to write to the shapefile; either 'data' (default) or 'duplicates'

        Returns
        -------
        """
        if table not in ['data', 'duplicates']:
            log.warning('Only data and duplicates can be exported!')
            return

        # add the .shp extension if missing
        if not path.endswith('.shp'):
            path += '.shp'

        # creates folder if not present, adds .shp if not within the path
        dirname = os.path.dirname(path)
        os.makedirs(dirname, exist_ok=True)

        # uses spatialist.ogr2ogr to write shps with given path (or db connection)
        if self.driver == 'sqlite':
            # ogr2ogr(self.dbfile, path, options={'format': 'ESRI Shapefile'})
            subprocess.call(['ogr2ogr', '-f', 'ESRI Shapefile', path,
                             self.dbname, table])

        if self.driver == 'postgresql':
            db_connection = """PG:host={0} port={1} user={2}
                dbname={3} password={4} active_schema=public""".format(self.url_dict['host'],
                                                                       self.url_dict['port'],
                                                                       self.url_dict['username'],
                                                                       self.url_dict['database'],
                                                                       self.url_dict['password'])
            # ogr2ogr(db_connection, path, options={'format': 'ESRI Shapefile'})
            subprocess.call(['ogr2ogr', '-f', 'ESRI Shapefile', path,
                             db_connection, table])

    def filter_scenelist(self, scenelist):
        """
        Filter a list of scenes by file names already registered in the database.
        Parameters
        ----------
        scenelist: :obj:`list` of :obj:`str` or :obj:`pyroSAR.drivers.ID`
            the scenes to be filtered
        Returns
        -------
        list
            the file names of the scenes whose basename is not yet registered in the database
        """
        for item in scenelist:
            if not isinstance(item, (ID, str)):
                raise TypeError("items in scenelist must be of type 'str' or 'pyroSAR.ID'")

        # ORM query, get all scenes locations
        scenes_data = self.Session().query(self.Data.scene)
        registered = [os.path.basename(self.encode(x[0])) for x in scenes_data]
        scenes_duplicates = self.Session().query(self.Duplicates.scene)
        duplicates = [os.path.basename(self.encode(x[0])) for x in scenes_duplicates]
        names = [item.scene if isinstance(item, ID) else item for item in scenelist]
        filtered = [x for x, y in zip(scenelist, names) if os.path.basename(y) not in registered + duplicates]
        return filtered

    def get_colnames(self, table='data'):
        """
        Return the names of all columns of a table.
        Returns
        -------
        list
            the column names of the chosen table
        """
        # get all columns of one table, but shows geometry columns not correctly
        table_info = Table(table, self.meta, autoload=True, autoload_with=self.engine)
        col_names = table_info.c.keys()

        return sorted([self.encode(x) for x in col_names])

    def get_tablenames(self, return_all=True):
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
        #  TODO: make this dynamic
        #  the method was intended to only return user generated tables by default, as well as data and duplicates
        all_tables = ['ElementaryGeometries', 'SpatialIndex', 'geometry_columns', 'geometry_columns_auth',
                      'geometry_columns_field_infos', 'geometry_columns_statistics', 'geometry_columns_time',
                      'spatial_ref_sys', 'spatial_ref_sys_aux', 'spatialite_history', 'sql_statements_log',
                      'sqlite_sequence', 'views_geometry_columns', 'views_geometry_columns_auth',
                      'views_geometry_columns_field_infos', 'views_geometry_columns_statistics',
                      'virts_geometry_columns', 'virts_geometry_columns_auth', 'virts_geometry_columns_field_infos',
                      'virts_geometry_columns_statistics', 'data_licenses', 'KNN']
        # get tablenames from metadata
        tables = sorted([self.encode(x) for x in self.meta.tables.keys()])
        if return_all:
            return tables
        else:
            ret = []
            for i in tables:
                if i not in all_tables and 'idx_' not in i:
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
        return [key.name for key in Table(table, self.meta,
                                          autoload=True,
                                          autoload_with=self.engine).primary_key]
    def get_unique_directories(self, table_name='data'):
        """
        Get a list of directories containing registered scenes
        Returns
        -------
        list
            the directory names
        """
        # ORM query, get all directories
        table = Table(table_name.lower(), self.meta, autoload=True, autoload_with=self.engine)
        scenes = self.Session().query(table.c.scene)
        registered = [os.path.dirname(self.encode(x[0])) for x in scenes]
        return list(set(registered))

    def move(self, scenelist, directory, pbar=False):
        """
        Move a list of files while keeping the database entries up to date.
        If a scene is registered in the database (in either the data or duplicates table),
        the scene entry is directly changed to the new location.
        Parameters
        ----------
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
            if self.select(scene=scene) != 0:
                table = 'data'
            else:
                # using core connection to execute SQL syntax (as was before)
                query_duplicates = self.conn.execute(
                    '''SELECT scene FROM duplicates WHERE scene='{0}' '''.format(scene))
                if len(query_duplicates) != 0:
                    table = 'duplicates'
                else:
                    table = None
            if table:
                # using core connection to execute SQL syntax (as was before)
                self.conn.execute('''UPDATE {0} SET scene= '{1}' WHERE scene='{2}' '''.format(table, new, scene))
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
            print additional info
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
                for rowproxy in self.archive.conn.execute('SELECT True FROM {} LIMIT 1;'.format(table))]) == 0:
            print('Table {} is empty!'.format(table))
            return []
        col_names = self.archive.get_colnames(table)

        # here the geometry_columns table is queried, it has info about all tables' geometry columns
        geom_list = [{column: value for column, value in rowproxy.items()}
                     for rowproxy in self.archive.conn.execute('SELECT f_table_name, f_geometry_column '
                                                               'FROM geometry_columns;')]
        geom_col_name = None
        for entry in geom_list:
            if table in entry.values():
                geom_col_name = entry['f_geometry_column']

        if isinstance(date, list):  # TODO: see how to query datetime. check if date exists
            mindate = date[0]
            maxdate = date[1]

        arg_valid = [x for x in args.keys() if x in col_names]
        arg_invalid = [x for x in args.keys() if x not in col_names]
        if len(arg_invalid) > 0:
            print('the following arguments will be ignored as they are not registered in the data base: {}'.format(
                ', '.join(arg_invalid)))

        if selected_columns != '*':
            if isinstance(selected_columns, str):
                selected_columns = [selected_columns]
            sel_col_invalid = [x for x in selected_columns if x not in col_names]
            if len(sel_col_invalid) > 0:
                print('the following selected columns will be ignored '
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
                    print('WARNING: argument mindate is ignored, must be in format YYYYmmddTHHMMSS')
            if key == 'maxdate':
                if re.search('[0-9]{8}T[0-9]{6}', args[key]):
                    arg_format.append('stop<=?')
                    vals.append(args[key])
                else:
                    print('WARNING: argument maxdate is ignored, must be in format YYYYmmddTHHMMSS')

        if vectorobject and geom_col_name:
            if isinstance(vectorobject, Vector):
                vectorobject.reproject('+proj=longlat +datum=WGS84 +no_defs ')
                site_geom = vectorobject.convert2wkt(set3D=False)[0]
                # postgres has a different way to store geometries
                if self.archive.driver == 'postgres':
                    arg_format.append("st_intersects({0}, 'SRID=4326; {1}')".format(
                        geom_col_name,
                        site_geom
                    ))
                else:
                    arg_format.append('st_intersects(GeomFromText(?, 4326), {}) = 1'.format(geom_col_name))
                    vals.append(site_geom)
            else:
                print('WARNING: argument vectorobject is ignored, must be of type spatialist.vector.Vector. '
                      'Check also if table has geom column!')

        query = '''SELECT {} FROM {}'''.format(', '.join(selected_columns), table)
        if len(arg_format) > 0:
            query += ''' WHERE {}'''.format(' AND '.join(arg_format))
        # the query gets assembled stepwise here
        for val in vals:
            query = query.replace('?', ''' '{0}' ''', 1).format(val)
        if verbose:
            print(query)
        # core SQL execution
        query_rs = self.archive.conn.execute(query)
        return [{column: value for column, value in rowproxy.items()} for rowproxy in query_rs]

    # def select(self, vectorobject=None, mindate=None, maxdate=None, processdir=None,
    #            recursive=False, polarizations=None, **args):
    #     """
    #     select scenes from the database
    #     Parameters
    #     ----------
    #     vectorobject: :class:`~spatialist.vector.Vector`
    #         a geometry with which the scenes need to overlap
    #     mindate: str or datetime.datetime, optional
    #         the minimum acquisition date; strings must be in format YYYYmmddTHHMMSS; default: None
    #     maxdate: str or datetime.datetime, optional
    #         the maximum acquisition date; strings must be in format YYYYmmddTHHMMSS; default: None
    #     processdir: str, optional
    #         A directory to be scanned for already processed scenes;
    #         the selected scenes will be filtered to those that have not yet been processed. Default: None
    #     recursive: bool
    #         (only if `processdir` is not None) should also the subdirectories of the `processdir` be scanned?
    #     polarizations: list
    #         a list of polarization strings, e.g. ['HH', 'VV']
    #     **args:
    #         any further arguments (columns), which are registered in the database. See :meth:`~Archive.get_colnames()`
    #     Returns
    #     -------
    #     list
    #         the file names pointing to the selected scenes
    #     """
    #     arg_valid = [x for x in args.keys() if x in self.get_colnames()]
    #     arg_invalid = [x for x in args.keys() if x not in self.get_colnames()]
    #     if len(arg_invalid) > 0:
    #         log.info('the following arguments will be ignored as they are not registered in the data base: {}'.format(
    #             ', '.join(arg_invalid)))
    #     arg_format = []
    #     vals = []
    #     for key in arg_valid:
    #         if key == 'scene':
    #             arg_format.append('''scene LIKE '%%{0}%%' '''.format(os.path.basename(args[key])))
    #         else:
    #             if isinstance(args[key], (float, int, str)):
    #                 arg_format.append('''{0}='{1}' '''.format(key, args[key]))
    #             elif isinstance(args[key], (tuple, list)):
    #                 arg_format.append('''{0} IN ('{1}')'''.format(key, "', '".join(map(str, args[key]))))
    #     if mindate:
    #         if isinstance(mindate, datetime):
    #             mindate = mindate.strftime('%Y%m%dT%H%M%S')
    #         if re.search('[0-9]{8}T[0-9]{6}', mindate):
    #             arg_format.append('start>=?')
    #             vals.append(mindate)
    #         else:
    #             log.info('WARNING: argument mindate is ignored, must be in format YYYYmmddTHHMMSS')
    #     if maxdate:
    #         if isinstance(maxdate, datetime):
    #             maxdate = maxdate.strftime('%Y%m%dT%H%M%S')
    #         if re.search('[0-9]{8}T[0-9]{6}', maxdate):
    #             arg_format.append('stop<=?')
    #             vals.append(maxdate)
    #         else:
    #             log.info('WARNING: argument maxdate is ignored, must be in format YYYYmmddTHHMMSS')
    #
    #     if polarizations:
    #         for pol in polarizations:
    #             if pol in ['HH', 'VV', 'HV', 'VH']:
    #                 arg_format.append('{}=1'.format(pol.lower()))
    #
    #     if vectorobject:
    #         if isinstance(vectorobject, Vector):
    #             vectorobject.reproject(4326)
    #             site_geom = vectorobject.convert2wkt(set3D=False)[0]
    #             # postgres has a different way to store geometries
    #             if self.driver == 'postgresql':
    #                 arg_format.append("st_intersects(bbox, 'SRID=4326; {}')".format(
    #                     site_geom
    #                 ))
    #             else:
    #                 arg_format.append('st_intersects(GeomFromText(?, 4326), bbox) = 1')
    #                 vals.append(site_geom)
    #         else:
    #             log.info('WARNING: argument vectorobject is ignored, must be of type spatialist.vector.Vector')
    #
    #     query = '''SELECT scene, outname_base FROM data WHERE {}'''.format(' AND '.join(arg_format))
    #     # the query gets assembled stepwise here
    #     for val in vals:
    #         query = query.replace('?', ''' '{0}' ''', 1).format(val)
    #     log.debug(query)
    #
    #     # core SQL execution
    #     query_rs = self.conn.execute(query)
    #
    #     if processdir and os.path.isdir(processdir):
    #         scenes = [x for x in query_rs
    #                   if len(finder(processdir, [x[1]], regex=True, recursive=recursive)) == 0]
    #     else:
    #         scenes = query_rs
    #     ret = []
    #     for x in scenes:
    #         ret.append(self.encode(x[0]))
    #
    #     return ret

    # def select_duplicates(self, outname_base=None, scene=None, value='id'):
    #     """
    #     Select scenes from the duplicates table. In case both `outname_base` and `scene` are set to None all scenes in
    #     the table are returned, otherwise only those that match the attributes `outname_base` and `scene` if they are not None.
    #     Parameters
    #     ----------
    #     outname_base: str
    #         the basename of the scene
    #     scene: str
    #         the scene name
    #     value: str
    #         the return value; either 'id' or 'scene'
    #     Returns
    #     -------
    #     list
    #         the selected scene(s)
    #     """
    #     if value == 'id':
    #         key = 0
    #     elif value == 'scene':
    #         key = 1
    #     else:
    #         raise ValueError("argument 'value' must be either 0 or 1")
    #
    #     if not outname_base and not scene:
    #         # core SQL execution
    #         scenes = self.conn.execute('SELECT * from duplicates')
    #     else:
    #         cond = []
    #         arg = []
    #         if outname_base:
    #             cond.append('outname_base=?')
    #             arg.append(outname_base)
    #         if scene:
    #             cond.append('scene=?')
    #             arg.append(scene)
    #         query = 'SELECT * from duplicates WHERE {}'.format(' AND '.join(cond))
    #         for a in arg:
    #             query = query.replace('?', ''' '{0}' ''', 1).format(a)
    #         # core SQL execution
    #         scenes = self.conn.execute(query)
    #
    #     ret = []
    #     for x in scenes:
    #         ret.append(self.encode(x[key]))
    #
    #     return ret

    # @property
    # def size(self):
    #     """
    #     get the number of scenes registered in the database
    #     Returns
    #     -------
    #     tuple
    #         the number of scenes in (1) the main table and (2) the duplicates table
    #     """
    #     # ORM query
    #     session = self.Session()
    #     r1 = session.query(self.Data.outname_base).count()
    #     r2 = session.query(self.Duplicates.outname_base).count()
    #     session.close()
    #     return r1, r2

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

    # def drop_element(self, scene, with_duplicates=False):
    #     """
    #     Drop a scene from the data table.
    #     If duplicates table contains matching entry, it will be moved to the data table.
    #     Parameters
    #     ----------
    #     scene: ID
    #         a SAR scene
    #     with_duplicates: bool
    #         True: delete matching entry in duplicates table
    #         False: move matching entry from duplicates into data table
    #     Returns
    #     -------
    #     """
    #     # save outname_base from to be deleted entry
    #     search = self.data_schema.select().where(self.data_schema.c.scene == scene)
    #     entry_data_outname_base = []
    #     for rowproxy in self.conn.execute(search):
    #         entry_data_outname_base.append((rowproxy[12]))
    #     # log.info(entry_data_outname_base)
    #
    #     # delete entry in data table
    #     delete_statement = self.data_schema.delete().where(self.data_schema.c.scene == scene)
    #     self.conn.execute(delete_statement)
    #
    #     return_sentence = 'Entry with scene-id: \n{} \nwas dropped from data'.format(scene)
    #
    #     # with_duplicates == True, delete entry from duplicates
    #     if with_duplicates:
    #         delete_statement_dup = self.duplicates_schema.delete().where(
    #             self.duplicates_schema.c.outname_base == entry_data_outname_base[0])
    #         self.conn.execute(delete_statement_dup)
    #
    #         log.info(return_sentence + ' and duplicates!'.format(scene))
    #         return
    #
    #     # else select scene info matching outname_base from duplicates
    #     select_in_duplicates_statement = self.duplicates_schema.select().where(
    #         self.duplicates_schema.c.outname_base == entry_data_outname_base[0])
    #     entry_duplicates_scene = []
    #     for rowproxy in self.conn.execute(select_in_duplicates_statement):
    #         entry_duplicates_scene.append((rowproxy[1]))
    #
    #     # check if there is a duplicate
    #     if len(entry_duplicates_scene) == 1:
    #         # remove entry from duplicates
    #         delete_statement_dup = self.duplicates_schema.delete().where(
    #             self.duplicates_schema.c.outname_base == entry_data_outname_base[0])
    #         self.conn.execute(delete_statement_dup)
    #
    #         # insert scene from duplicates into data
    #         self.insert(entry_duplicates_scene[0])
    #
    #         return_sentence += ' and entry with outname_base \n{} \nand scene \n{} \n' \
    #                            'was moved from duplicates into data table'.format(
    #             entry_data_outname_base[0], entry_duplicates_scene[0])
    #
    #     log.info(return_sentence + '!')

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
            table_info = Table(table, self.meta, autoload=True, autoload_with=self.engine)
            table_info.drop(self.engine)
            log.info('table {} dropped from database.'.format(table))
        else:
            raise ValueError("table {} is not registered in the database!".format(table))
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

    def ingest_sentinel2_from_folder(self, scene_dirs, update=False, verbose=False):
        """
        Method to open Sentinel-2 .zips with the vsizip in GDAL to read out metadata and ingest them in the
        table sentinel2data.

        Parameters
        ----------
        scene_dirs: str
            list of Sentinel-2 zip paths
        update: bool
            update database? will overwrite matching entries
        verbose: bool
            print additional info
        s2_scene_titles: list (str)
            list of sentinel2 titles (filename without extension) to ingest,
            if provided will only add those to the db if there are other not reqired scenes in the folder

        Returns
        -------

        """
        metadata = []
        tmp = os.environ.get('CPL_ZIP_ENCODING')
        os.environ['CPL_ZIP_ENCODING'] = 'UTF-8'
        for filename in scene_dirs:
            name_dot_safe = Path(filename).stem + '.SAFE'
            xml_file = None
            if filename[4:10] == 'MSIL2A':
                xml_file = gdal.Open(
                    '/vsizip/' + os.path.join(filename, name_dot_safe, 'MTD_MSIL2A.xml'))
            elif filename[4:10] == 'MSIL1C':  # todo check if this actually works with L1C
                xml_file = gdal.Open(
                    '/vsizip/' + os.path.join(filename, name_dot_safe, 'MTD_MSIL1C.xml'))
            # this way we can open most raster formats and read metadata this way, just adjust the ifs..
            if xml_file:
                metadata.append([filename, xml_file])
                xml_file = None

        orderly_data = self.__refactor_sentinel2data(metadata)
        # todo check if footprint has correct projection
        self.insert(table='sentinel2data', primary_key=self.get_primary_keys('sentinel2data'),
                    orderly_data=orderly_data, verbose=verbose, update=update)

        # os.environ.['CPL_ZIP_ENCODING'] = tmp  # does not work somehow
        return metadata

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
        table_info = Table('sentinel2data', self.archive.meta, autoload=True, autoload_with=self.archive.engine).c
        coltypes = {}
        for i in table_info:
            coltypes[i.name] = i.type

        orderly_data = []
        for entry in metadata_as_list_of_dicts:
            temp_dict = {}
            for key, value in entry[1].GetMetadata().items():
                key = key.lower().replace(' ', '_')
                if str(coltypes.get(key)) == 'VARCHAR':
                    temp_dict[key] = value
                if str(coltypes.get(key)) == 'INTEGER':
                    temp_dict[key] = int(value.replace('.0', ''))
                if str(coltypes.get(key)) in ['DOUBLE PRECISION', 'FLOAT']:
                    temp_dict[key] = float(value)
                if str(coltypes.get(key)) in ['TIMESTAMP', 'TIMESTAMP WITHOUT TIME ZONE', 'DATETIME']:
                    if value != '' and value:
                        temp_dict[key] = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ')
                if str(coltypes.get(key)) in ['NUMERIC']:
                    temp_dict[key] = WKTElement(value, srid=4326)

            temp_dict['outname_base'] = entry[0]
            orderly_data.append(temp_dict)
        return orderly_data


def drop_archive(archive):
    """
    drop (delete) a scene database

    Parameters
    ----------
    archive: pyroSAR.drivers.Archive
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

    >>> db = Archive('test', postgres=True, port=5432, user=pguser, password=pgpassword)
    >>> drop_archive(db)
    """
    url = archive.url
    archive.close()
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
        print('ERROR, no tables found to create!')

    return tables
