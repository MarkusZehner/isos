import os
from pathlib import Path

from osgeo import gdal
from spatialist.ancillary import finder

pattern_s1 = '^S1[AB]_.*.zip$'
pattern_s2 = '^S2[AB]_MSI.*.zip$'

scene_dirs = finder('/geonfs03_vol1/', ['^S2[AB]_MSI.*.zip$'], regex=True, recursive=True)

# add scene dirs to a ingest

with open("/geonfs03_vol1/THURINGIA/s2_scenes_on_geonfs03_vol1_2022_01_07.txt", "w") as output:
    output.write(str(scene_dirs))




#### sentinel 2

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
