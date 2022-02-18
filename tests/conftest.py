import os
import pytest


@pytest.fixture
def travis():
    return 'TRAVIS' in os.environ.keys()


@pytest.fixture
def appveyor():
    return 'APPVEYOR' in os.environ.keys()


@pytest.fixture
def testdir():
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')


@pytest.fixture
def testdata(testdir):
    out = {
        's1': os.path.join(testdir, 'S1A_IW_GRDH_1SDV_20150222T170750_20150222T170815_004739_005DD8_3768.zip'),
        's1_2': os.path.join(testdir, 'S1A_IW_GRDH_1SDV_20150222T170725_20150222T170750_004739_005DD8_CEAB.zip'),
        's2': os.path.join(testdir, 'S2B_MSIL2A_20220117T095239_N0301_R079_T32QMG_20220117T113605.zip'),
        's2_2': os.path.join(testdir, 'S2B_MSIL2A_20220117T095239_N0301_R079_T33UYR_20220117T113605.zip'),
        's2_3': os.path.join(testdir, 'S2A_MSIL1C_20191228T144721_N0208_R139_T19MGQ_20191228T163224.zip'),
        's1_dup': os.path.join(testdir, 'duplicates',
                               'S1A_IW_GRDH_1SDV_20150222T170750_20150222T170815_004739_005DD8_3768.zip'),
        's1_2_dup': os.path.join(testdir, 'duplicates',
                                 'S1A_IW_GRDH_1SDV_20150222T170725_20150222T170750_004739_005DD8_CEAB.zip'),
        's2_dup': os.path.join(testdir, 'duplicates',
                               'S2B_MSIL2A_20220117T095239_N0301_R079_T32QMG_20220117T113605.zip'),
        's2_2_dup': os.path.join(testdir, 'duplicates',
                                 'S2B_MSIL2A_20220117T095239_N0301_R079_T33UYR_20220117T113605.zip')
    }
    return out


@pytest.fixture
def auxdata_dem_cases():
    cases = [('AW3D30', ['N050E010/N051E011.tar.gz']),
             ('SRTM 1Sec HGT', ['N51E011.SRTMGL1.hgt.zip']),
             ('SRTM 3Sec', ['srtm_39_02.zip']),
             ('TDX90m', ['90mdem/DEM/N51/E010/TDM1_DEM__30_N51E011.zip'])]
    return cases
