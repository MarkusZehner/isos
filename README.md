# isos
in search of sentinel is intended for data management of sentinel-1 and 2 scenes.


##install via

    $ pip install git+https://github.com/MarkusZehner/isos.git
    $ pip install git+https://github.com/MarkusZehner/pyroSAR.git@add_geometry

Also install gdal...

##General usage:

Isos relies on a postgres server running with postgis extension. 
Data of known Sensors are detected via file search, and are ingested into a metadata table that can be adressed via pyroSAR:

```python
# this should be running as a cronjob in the background:

from isos import filesweeper, ingest_from_exist_table

filesweeper('some/dir/', 'user', 'password', port=8888)

ingest_from_exist_table('user', 'password', port=8888)


# access from pyroSAR just as normal, with table statement..
# oh, this will create tables data and duplicates.. oh well...
from pyroSAR import Archive

with Archive('isos_db', postgres=True, 
             user='user', password='password', 
             port=8888, add_geometry=True) as db:
    # define normal search parameter, but change table to 'sentinel1data'!
    db.select(vectorobject=None, mindate=None, 
              maxdate=None, processdir=None,
              recursive=False, polarizations=None, 
              use_geometry=False, table='sentinel1data')
```