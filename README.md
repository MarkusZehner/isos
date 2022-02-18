# Isos
In search of sentinel is intended for data management of Sentinel-1 and 2 scenes.
Isos relies on a postgresql server running with postgis extension within a Singularity container, 
and a secondary container with the python module installed.
Data of known Sensors are detected via file search and ingested into a metadata table that can be addressed via pyroSAR.


## General usage:
The main process is running at a two-day interval at 11PM via crontab:

```bash
#-----------------------------------------------------------------------------
#Min     Hour    Day     Month   Weekday Command
#-----------------------------------------------------------------------------
0       23      */2     *       *       /usr/local/bin/singularity exec -e -c --bind /.../isos_scripts:/tmp,/search_dir:/search_dir /.../isos_py.sif bash /tmp/exec_script.sh /search_dir/ dbname user 1234 8888
```

## connect with pyroSAR: 
This requires the below stated branch of pyroSAR.
```python
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


## installation and setup:
Place the provided environment at /aux/testsingenv.yml in the same folder with the .def files, 
create folders isos_scripts, pg_run, and pg_data. 
Then run following to create the singularity containers:

    $ singularity build (--sandbox) isos_postgres.sif py_sing_second_try.def
    $ singularity build (--sandbox) isos_postgres.sif postgis.def

Run the containers:

    $ nohup singularity run  -c --bind pg_data:/var/lib/postgresql/data,pg_run:/run/postgresql/ isos_postgres_container.sif &
    $ singularity exec -e -c --bind /isos_scripts:/tmp,/search_dir:/search_dir /isos_py.sif bash /tmp/exec_script.sh /search_dir/ dbname user 1234 8888

Manual Installation via pip:

    $ pip install git+https://github.com/MarkusZehner/isos.git
    $ pip install git+https://github.com/MarkusZehner/pyroSAR.git@add_geometry


