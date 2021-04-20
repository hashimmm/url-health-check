import psycopg2
import psycopg2.pool
import settings

pools = {}


def close_all():
    """Close all connections.

    For use at the end of the applications run.
    """
    for pool in pools.values():
        pool.closeall()


class DBHelper:
    def __init__(self, cur, conn) -> None:
        self.cur = cur
        self.conn = conn
    
    def execute(self, *args, **kwargs):
        self.cur.execute(*args, **kwargs)
        return self.cur

    def commit(self, *args, **kwargs):
        return self.conn.commit(*args, **kwargs)

    def rollback(self, *args, **kwargs):
        return self.conn.rollback(*args, **kwargs)


class get_cursor:  # Snake-cased since usage looks like function.
    def __init__(self, connection_uri=settings.POSTGRES_URI):
        # NOTE: not threadsafe.
        if connection_uri not in pools:
            # We really will just be using 1 connection throughout.
            pools[connection_uri] = psycopg2.pool.SimpleConnectionPool(1, 3, connection_uri)
        self.connection_uri = connection_uri

    def __enter__(self):
        self.connection = pools[self.connection_uri].getconn()
        self.cur = self.connection.cursor()
        return DBHelper(self.cur, self.connection)

    def __exit__(self, *exc_tuple):
        self.cur.close()
        pools[self.connection_uri].putconn(self.connection)


def ensure_table(connection_uri=settings.POSTGRES_URI):
    """Make sure the table exists.

    Ideally used at the start of the application, or separately.
    """
    with get_cursor(connection_uri=connection_uri) as dbh:
        dbh.execute(
            """CREATE TABLE IF NOT EXISTS health_check_data (
            id BIGSERIAL PRIMARY KEY,
            recorded_at TIMESTAMP WITH TIME ZONE,
            url TEXT NOT NULL,
            status_code INTEGER,
            status TEXT NOT NULL,
            response_time_secs real
        )
        ;"""
        )
        # Indexing follows recommendations from:
        # https://aws.amazon.com/blogs/database/designing-high-performance-time-series-data-tables-on-amazon-rds-for-postgresql/
        # Will need to benchmark to know for sure.
        dbh.execute(
            """CREATE INDEX IF NOT EXISTS health_check_recorded_at_brin_idx
                ON health_check_data
            USING BRIN (recorded_at)
            WITH (pages_per_range = 32);"""
        )
        dbh.execute(
            """CREATE INDEX IF NOT EXISTS health_check_url_recorded_at_idx ON health_check_data 
                USING btree (url, recorded_at DESC);"""
        )
        dbh.commit()
