import sys
from isos import filewalker

if __name__ == '__main__':
    directory = sys.argv[1]
    print(directory)
    dbname    = sys.argv[2]
    user      = sys.argv[3]
    password  = sys.argv[4]
    port      = int(sys.argv[5])

    filewalker.cronjob_task(directory, dbname, user, password, port)
