"""
helper functions needed when interacting with the Postgres database
"""
from decorator import decorator
import psycopg2


def pg_conn(func, schema, **conn_kwds):
    """handles the psycopg2 conn/cursor for a function

    Parameters
    ----------
    func : function
        method to decorate
    schema : str
        schema for the connection
    conn_kwds : dict
        additional key words for the connection

    Returns
    -------
    function
        decorated to handle conn/cursor
    """

    def nfunc(func, *args, **kwds):

        conn = psycopg2.connect(**conn_kwds)
        cursor = conn.cursor()
        cursor.execute("SET search_path TO %s" % schema)
        conn.commit()

        kwds["conn"] = conn
        kwds["cursor"] = cursor
        res = func(*args, **kwds)

        cursor.close()
        conn.close()

        return res

    return decorator(nfunc)(func)


def create_schema(schema, **conn_kwds):
    """creates the schema in the corresponding database

    Parameters
    ----------
    schema : str
        name of schema
    conn_kwds : dict
        additional key words for the connection

    Notes
    -----
    Only creates the schema if it doesn't already exist
    """

    conn = psycopg2.connect(**conn_kwds)
    cursor = conn.cursor()
    cursor.execute("CREATE SCHEMA IF NOT EXISTS %s;" % schema)
    conn.commit()
    cursor.close()
    conn.close()
