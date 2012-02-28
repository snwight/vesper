#:copyright: Copyright 2009-2011 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
"""
    SqlMapping model based utility which produces a simple JSON mapping
    from a schema derived by inspecting the database named in input argument
"""
import os, sys
from vesper.data.store.sqlmapping import SqlMappingStore


def mapit(argv):
    print [argv[i] for i in range(len(argv))]
    argc = len(argv)
    print argc
    if argc < 5:
        print "usage: mappingGenerator <'mysql'|'pgsql'|'sqlite'> <user_name> <password> <schema_name> ['localhost'|<hostname>] [<port_number>]"
        return
    db = argv[1]
    user = argv[2]
    pwd = argv[3]
    dbName = argv[4]
    if argc > 5:
        host = argv[5]
    else:
        host = 'localhost'

    # format arguments as SQLAlchemy configuration string
    driver = None
    dbConfiguration = None
    if db == 'sqlite':
        driver = os.getenv("SQLA_SQLITE_DRIVER")
        dbConfiguration = "{0}:///{1}".format(driver, dbName)
    elif db == 'mysql':
        driver = os.getenv("SQLA_MYSQL_DRIVER")
        if argc > 5:
            port = int(argv[6])
        else:
            port = 3306
        dbConfiguration = "{0}://{1}:{2}@{3}:{4}/{5}".format(driver, user, pwd, host, port, dbName)
    elif db == 'pgsql':
        driver = os.getenv("SQLA_PGSQL_DRIVER")
        if argc > 5:
            port = int(argv[5])
        else:
            port = 5432
        dbConfiguration = "{0}://{1}:{2}@{3}:{4}/{5}".format(driver, user, pwd, host, port, dbName)

    # creation of this model will generate the desired mapping into self.mapping
    print dbConfiguration
    SqlMappingStore(source=dbConfiguration, loadVesperTable=False)


if __name__ == "__main__":
    mapit(sys.argv)
