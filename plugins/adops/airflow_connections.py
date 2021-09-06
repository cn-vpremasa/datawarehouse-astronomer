from airflow import settings
from airflow.models import Connection

def create_airflow_connection(conn_id, conn_type,host, login, password, port, extra, uri, force_create=False):
    """
    Reference: https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html
    """
    conn = Connection(
            conn_id=conn_id,
            conn_type=conn_type,
            host=host,
            login=login,
            password=password,
            port=port,
            extra=extra,
            uri=uri
    )

    session = settings.Session

    if (force_create) or (conn_id not in [str(c) for c in list_connections(session)]):
        session = settings.Session
        session.add(conn)
        session.commit()
        session.close()

def list_connections(session):

    _session = None

    if session is not None:
        _session = session
    else:
        _session = settings.Session

    _c = _session.query(Connection).all()

    return _c

