#:copyright: Copyright 2009-2011 by the Vesper team, see AUTHORS.
#:license: Dual licenced under the GPL or Apache2 licences, see LICENSE.
"""
    SqlMapping model based utility which produces a simple JSON mapping
    from a schema derived by inspecting the database named in input argument
"""
import os, sys
from vesper.data.store.sqlmapping import SqlMappingStore


def mapit(self, db=None, user=None, pwd=None, host=None, port=None, dbName=None):
    # format arguments as SQLAlchemy configuration string
    print db, user, pwd, host, port, dbName
    driver = None
    dbConfiguration = None
    if db is None:
        driver = os.getenv("SQLA_SQLITE_DRIVER")
        dbConfiguration = "{0}:///{1}".format(driver, dbName)
    else:
        if host is None:
            host = 'localhost'
        if db == 'mysql':
            driver = os.getenv("SQLA_MYSQL_DRIVER")
            if port is None:
                port = 3306
        elif db == 'pgsql':
            driver = os.getenv("SQLA_PGSQL_DRIVER")
            if port is None:
                port = 5432
        dbConfiguration = "{0}://{1}:{2}@{3}:{4}/{5}".format(driver, user, pwd, host, port, dbName)

    # creation of this model will generate the desired mapping into self.mapping
    print dbConfiguration
    SqlMappingStore(source=dbConfiguration, loadVesperTable=False)


if __name__ == "__main__":
    mapit(sys.argv[0], sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], int(sys.argv[5]), sys.argv[6]) 
