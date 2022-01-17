# search partition for all S1, s2 raw data regularly,
# get paths, store into database and check if some went missing
import os
from spatialist.ancillary import finder
from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt


# start or stop postgresql server
#subprocess('pg_ctl -D /opt/homebrew/var/postgres start')

#user = 'zehma'
#password = os.getenv('sentinelsat_API_KEY')
# find all s2l1c on one partition:






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

