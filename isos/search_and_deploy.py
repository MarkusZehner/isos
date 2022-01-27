# search partition for all S1, s2 raw data regularly,
# get paths, store into database and check if some went missing
#import os
#from spatialist.ancillary import finder
#from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt


# start or stop postgresql server
#subprocess('pg_ctl -D /opt/homebrew/var/postgres start')

#user = 'zehma'
#password = os.getenv('sentinelsat_API_KEY')
# find all s2l1c on one partition:



# crontab every second day:
# 0 0 2-30/2 * * /pathtopythonenv /command
import sys
import os

from spatialist.ancillary import finder
from isos import Database
# from datetime import datetime
# import os


def main(directory, user=False):

    pguser = 'user'  # os.environ.get('PGUSER')
    pgpassword = 'password'  # os.environ.get('PGPASSWORD')
    pgport = 8888  # os.environ.get('PGPORT')

    scenes_s1 = finder(directory, [r'^S1[AB].*\.zip'], recursive=True, regex=True)
    scenes_s2 = finder(directory, [r'^S2[AB].*\.zip'], recursive=True, regex=True)

    with Database('isos_db', user=pguser, password=pgpassword, port=pgport) as db:
        db.ingest_s1_from_id(scenes_s1)
        db.ingest_s2_from_id(scenes_s2)
        count1 = db.count_scenes('sentinel1data')
        print(count1)
        count2 = db.count_scenes('sentinel2data')
        print(count2)




#
#
#
# def write_file(filename, data):
#     if os.path.isfile(filename):
#         with open(filename, 'a') as f:
#             f.write('\n' + data)
#     else:
#         with open(filename, 'w') as f:
#             f.write(data)
#
#
# def print_time():
#     now = datetime.now()
#     current_time = now.strftime("%H:%M:%S")
#     data = "Current Time = " + current_time
#     return data
#
#
# write_file('test.txt', print_time())

if __name__ == "__main__":
    directory = sys.argv[1]
    print(directory)
    main(directory)

# optimally use extent to see where the data is, to be searchable by sensor, metadata and area

# additionally, orchestrate downloads from asf and copernicushub


# dl from copernicushub via sentinelsat


#api = SentinelAPI(user, password, 'https://apihub.copernicus.eu/apihub')
#footprint = geojson_to_wkt(read_geojson('/Users/markuszehner/Documents/hainich/Hainich_bbox_wgs84_simple.geojson'))
#products = api.query(footprint,
#                     date = ('20151219', '20151229'),
#                     platformname = 'Sentinel-2',
#                     cloudcoverpercentage = (0, 30))
#api.download_all(products, directory_path=directory_path)

