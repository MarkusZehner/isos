# -*- coding: utf-8 -*-
"""
@author: Markus Zehner

Setup for a Database
This file contains the structure of the database.
Each table is structured within a Class with declarative Base.

"""

from sqlalchemy import Column, Integer, String, DateTime, Float, Boolean, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry

Base = declarative_base()


# class Sentinel2Meta(Base):
#     """
#     Template for the Sentinel2Meta Table to record metadata for query checks
#     """
#     __tablename__ = 'sentinel2meta'
#
#     id = Column(String, primary_key=True)  # ': '8a7e6f5b-d53b-40f9-a215-afce648eae65',
#     title = Column(String)  # ': 'S2A_MSIL1C_20151228T100422_N0201_R122_T33UUQ_20151228T100420',
#     size = Column(Integer)  # ': 101067588,
#     md5 = Column(String)  # ': '412C32698318AFC6F46D53DA18967365',
#     date = Column(DateTime)  # ': datetime.datetime(2015, 12, 28, 10, 4, 22,29000),
#     footprint = Column(Geometry('POLYGON', management=True,
#                                 srid=4326))  # ': 'POLYGON((12.23093206 49.61959688,12.28537105 48.63303182,13.77513104 48.65851892,13.7505374 49.64598097,12.23093206 49.61959688,12.23093206 49.61959688))',
#     url = Column(
#         String)  # ': "https://scihub.copernicus.eu/apihub/odata/v1/Products('8a7e6f5b-d53b-40f9-a215-afce648eae65')/$value",
#     online = Column(Boolean)  # ': False,
#     creation_date = Column(DateTime)  # ': datetime.datetime(2018, 12, 20, 18, 59, 43, 165000),
#     ingestion_date = Column(DateTime)  # ': datetime.datetime(2018, 12, 20, 0, 39, 11, 379000)
#     aot_retrieval_accuracy = Column(Float)  # 0.0)
#     cloud_cover_percentage = Column(Float)  # 98.878347)
#     cloud_shadow_percentage = Column(Float)  # 0.0)
#     dark_features_percentage = Column(Float)  # 0.023467)
#     datatake_sensing_start = Column(DateTime)
#     degraded_ancillary_data_percentage = Column(Float)  # 0.0)
#     degraded_msi_data_percentage = Column(Float)  # 0)
#     filename = Column(String)  # 'S2A_MSIL2A_20200109T101401_N0213_R022_T33UUR_20200109T114354.SAFE')
#     format = Column(String)  # 'SAFE')
#     format_correctness = Column(String)  # 'PASSED')
#     general_quality = Column(String)  # 'PASSED')
#     generation_time = Column(DateTime)  # datetime.datetime(2020, 1, 9, 11, 43, 54))
#     geometric_quality = Column(String)  # 'PASSED')
#     high_proba_clouds_percentage = Column(Float)  # 13.026819)
#     identifier = Column(String)  # 'S2A_MSIL2A_20200109T101401_N0213_R022_T33UUR_20200109T114354')
#     instrument = Column(String)  # 'MSI')
#     instrument_abbreviation = Column(String)  # 'MSI')
#     instrument_mode = Column(String)
#     instrument_name = Column(String)  # 'Multi-Spectral Instrument')
#     jts_footprint = Column(Geometry('MULTIPOLYGON', management=True,
#                                     srid=4326))  # 'MULTIPOLYGON (((12.235902710246984 49.53173120782476, 13.752783043838743 49.558033952412664, 13.726924416029938 50.545333158635955, 12.178667516417683 50.51810003852152, 12.235902710246984 49.53173120782476)))')
#     medium_proba_clouds_percentage = Column(Float)  # 83.750689)
#     mission_datatake_id = Column(String)  # 'GS2A_20200109T101401_023757_N02.13')
#     no_data_pixel_percentage = Column(Float)  # 0.0)
#     not_vegetated_percentage = Column(Float)  # 0.000531)
#     nssdc_identifier = Column(String)  # '2015-028A')
#     orbit_number_start = Column(Integer)  # 23757)
#     pass_direction = Column(String)  # 'DESCENDING')
#     platform_serial_identifier = Column(String)  # 'Sentinel-2A')
#     processing_baseline = Column(Float)  # 2.13)
#     processing_level = Column(String)  # 'Level-2A')
#     product_type = Column(String)  # 'S2MSI2A')
#     radiometric_quality = Column(String)  # 'PASSED')
#     relative_orbit_start = Column(Integer)  # 22)
#     satellite = Column(String)  # 'Sentinel-2')
#     satellite_name = Column(String)  # 'Sentinel-2')
#     satellite_number = Column(String)  # 'A')
#     saturated_defective_pixel_percentage = Column(Float)  # 0.0)
#     sensing_start = Column(DateTime)  # datetime.datetime(2020, 1, 9, 10, 14, 1, 24000))
#     sensing_stop = Column(DateTime)  # datetime.datetime(2020, 1, 9, 10, 14, 1, 24000))
#     sensor_quality = Column(String)  # 'PASSED')
#     snow_ice_percentage = Column(Float)  # 1.072588)
#     thin_cirrus_percentage = Column(Float)  # 2.100839)
#     tile_identifier = Column(String)
#     tile_identifier_horizontal_order = Column(String)
#     unclassified_percentage = Column(Float)  # 0.024874000000000004)
#     vegetation_percentage = Column(Float)  # 0.0)
#     water_percentage = Column(Float)  # 0.000192)
#     water_vapour_retrieval_accuracy = Column(Float)  # 0.0)
#     level_1c_pdi_identifier = Column(String)  # 'S2A_OPER_MSI_L1C_TL_MTI__20200109T104556_A023757_T33UUR_N02.08')


class Sentinel2Data(Base):
    """
    Template for the Sentinel2Data Table to record metadata of downloaded optical imagery
    """
    __tablename__ = 'sentinel2data'

    outname_base = Column(String)
    scene = Column(String, primary_key=True)
    aot_quantification_value = Column(Float)  # 1000.0
    aot_quantification_value_unit = Column(String)  # none
    aot_retrieval_accuracy = Column(Float)  # 0.0
    boa_quantification_value = Column(Integer)  # 10000
    boa_quantification_value_unit = Column(String)  # none
    cloud_coverage_assessment = Column(Float)  # 95.271085
    cloud_shadow_percentage = Column(Float)  # 0.0
    dark_features_percentage = Column(Float)  # 0.206587
    datatake_1_datatake_sensing_start = Column(DateTime)  # 2020-02-02T10: 41:49.024Z
    datatake_1_datatake_type = Column(String)  # INS - NOBS
    datatake_1_id = Column(String)  # GS2B_20200202T104149_015192_N02.13
    datatake_1_sensing_orbit_direction = Column(String)  # DESCENDING
    datatake_1_sensing_orbit_number = Column(Integer)  # 8
    datatake_1_spacecraft_name = Column(String)  # Sentinel - 2B
    degraded_anc_data_percentage = Column(Float)  # 0.0
    degraded_msi_data_percentage = Column(Float)  # 0
    footprint = Column(Geometry('POLYGON', management=True,
                                srid=4326))  # POLYGON((8.924070006452805 51.44989927211676, 8.904384766189844 51.41224380557849, 8.829856885246626 51.268165451969296, 8.755706184813015 51.12399605276732, 8.682113020865213 50.97981217882954, 8.60877008625333 50.835685919241854, 8.53626004021424 50.6915098617924, 8.46471176454714 50.54718390934653, 8.421249074548209 50.45980342913615, 7.590721585607627 50.455265339610506, 7.560547850091258 51.44234525292824, 8.924070006452805 51.44989927211676))
    format_correctness = Column(String)  # PASSED
    general_quality = Column(String)  # PASSED
    generation_time = Column(DateTime)  # 2020 - 02 - 02T12: 31:31.000000Z
    geometric_quality = Column(String)  # PASSED
    high_proba_clouds_percentage = Column(Float)  # 71.275556
    medium_proba_clouds_percentage = Column(Float)  # 20.76612
    nodata_pixel_percentage = Column(Float)  # 29.950076
    not_vegetated_percentage = Column(Float)  # 0.007872
    preview_geo_info = Column(String)  # Not applicable
    preview_image_url = Column(String)  # Not applicable
    processing_baseline = Column(Float)  # 02.13
    processing_level = Column(String)  # Level - 2A
    product_start_time = Column(DateTime)  # 2020 - 02 - 02T10: 41:49.024Z
    product_stop_time = Column(DateTime)  # 2020 - 02 - 02T10: 41:49.024Z
    product_type = Column(String)  # S2MSI2A
    product_uri = Column(String)  # S2B_MSIL2A_20200202T104149_N0213_R008_T32UMB_20200202T123131.SAFE
    radiative_transfer_accuracy = Column(Float)  # 0.0
    radiometric_quality = Column(String)  # PASSED
    reflectance_conversion_u = Column(Float)  # 1.03090709722802
    saturated_defective_pixel_percentage = Column(Float)  # 0.0
    sensor_quality = Column(String)  # PASSED
    snow_ice_percentage = Column(Float)  # 4.49639
    special_value_nodata = Column(Integer)  # 0
    special_value_saturated = Column(Integer)  # 65535
    thin_cirrus_percentage = Column(Float)  # 3.229409
    unclassified_percentage = Column(Float)  # 0.015364999999999998
    vegetation_percentage = Column(Float)  # 0.0
    water_percentage = Column(Float)  # 0.002704
    water_vapour_retrieval_accuracy = Column(Float)  # 0.0
    wvp_quantification_value = Column(Float)  # 1000.0
    wvp_quantification_value_unit = Column(String)  # cm


class Sentinel1Data(Base):
    """
    Template for the Sentinel1Data Table to record metadata of downloaded SAR imagery
    """
    __tablename__ = 'sentinel1data'

    sensor = Column(String)
    orbit = Column(String)
    orbitNumber_abs = Column(Integer)
    orbitNumber_rel = Column(Integer)
    cycleNumber = Column(Integer)
    frameNumber = Column(Integer)
    acquisition_mode = Column(String)
    start = Column(String)
    stop = Column(String)
    product = Column(String)
    samples = Column(Integer)
    lines = Column(Integer)
    outname_base = Column(String)
    scene = Column(String, primary_key=True)
    hh = Column(Integer)
    vv = Column(Integer)
    hv = Column(Integer)
    vh = Column(Integer)
    bbox = Column(Geometry(geometry_type='POLYGON', management=True, srid=4326))
    geometry = Column(Geometry(geometry_type='POLYGON', management=True, srid=4326))


class Duplicates(Base):
    """
    should stay empty because of the complete path as primary key!
    """
    __tablename__ = 'duplicates'

    scene = Column(String, primary_key=True)
    outname_base = Column(String)


class ExistingS1(Base):
    """
    get all scenes on drive, check if accessible
    """
    __tablename__ = 'existings1'

    scene = Column(String, primary_key=True)
    outname_base = Column(String)
    read_permission = Column(Integer)
    file_size_MB = Column(Integer)
    owner = Column(String)


class ExistingS2(Base):
    """
    get all scenes on drive, check if accessible
    """
    __tablename__ = 'existings2'

    scene = Column(String, primary_key=True)
    outname_base = Column(String)
    read_permission = Column(Integer)
    file_size_MB = Column(Integer)
    owner = Column(String)
