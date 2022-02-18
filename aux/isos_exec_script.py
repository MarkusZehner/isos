import sys
from isos.search_and_deploy import cronjob_task

if __name__ == '__main__':
    directory = sys.argv[1]
    print(directory)
    dbname    = sys.argv[2]
    user      = sys.argv[3]
    password  = sys.argv[4]
    port      = int(sys.argv[5])

    cronjob_task(directory, dbname, user, password, port)
