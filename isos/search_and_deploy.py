# todo: search partition for all S1, s2 raw data regularly, get paths, store into database and check if some went missing


# find all s2l1c on one partition:

from spatialist.ancillary import finder

scenes_s2 = finder('/geonfs03_vol1/', ['^S2[AB]_MSI.*.zip$'], regex=True, recursive=True)

print(scenes_s2)

with open("/geonfs03_vol1/THURINGIA/s2_scenes_on_geonfs03_vol1_2022_01_07.txt", "w") as output:
    output.write(str(scenes_s2))



# optimally use extent to see where the data is, to be searchable by sensor, metadata and area

# additionally, orchestrate downloads from asf and copernicushub


# dl from copernicushub via sentinelsat
from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt

api = SentinelAPI(user, password, 'https://apihub.copernicus.eu/apihub')
footprint = geojson_to_wkt(read_geojson('/Users/markuszehner/Documents/hainich/Hainich_bbox_wgs84_simple.geojson'))
products = api.query(footprint,
                     date = ('20151219', '20151229'),
                     platformname = 'Sentinel-2',
                     cloudcoverpercentage = (0, 30))
api.download_all(products, directory_path=directory_path)