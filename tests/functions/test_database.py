import pytest
import sqlalchemy as sa
from flexmock import flexmock

from sqlalchemy_utils import create_database, database_exists, drop_database

pymysql = None
try:
    import pymysql  # noqa
except ImportError:
    pass


class DatabaseTest(object):
    def test_create_and_drop(self, dsn, db_name):
        url = sa.engine.url.make_url(dsn)
        db_name = url.database or db_name
        if url.drivername != 'sqlite':
            url.database = None
        kwargs = {}
        if url.drivername == 'postgresql+pg8000':
            kwargs = {'isolation_level': 'AUTOCOMMIT'}
        elif url.drivername == 'mssql+pyodbc':
            kwargs = {'connect_args': {'autocommit': True}}
            url = str(url)
        engine = sa.create_engine(url, **kwargs)
        assert not database_exists(engine, db_name)
        create_database(engine, db_name)
        assert database_exists(engine, db_name)
        drop_database(engine, db_name)
        assert not database_exists(engine, db_name)


@pytest.mark.usefixtures('sqlite_memory_dsn')
class TestDatabaseSQLiteMemory(object):
    def test_exists_memory(self, dsn):
        engine = sa.create_engine(dsn)
        assert database_exists(engine, ':memory:')


@pytest.mark.usefixtures('sqlite_none_database_dsn')
class TestDatabaseSQLiteMemoryNoDatabaseString(object):
    def test_exists_memory_none_database(self, sqlite_none_database_dsn):
        engine = sa.create_engine(sqlite_none_database_dsn)
        assert database_exists(engine, engine.url.database)


@pytest.mark.usefixtures('sqlite_file_dsn')
class TestDatabaseSQLiteFile(DatabaseTest):
    def test_existing_non_sqlite_file(self, dsn, db_name):
        database = sa.engine.url.make_url(dsn).database
        open(database, 'w').close()
        self.test_create_and_drop(dsn, database)


@pytest.mark.skipif('pymysql is None')
@pytest.mark.usefixtures('mysql_dsn')
class TestDatabaseMySQL(DatabaseTest):
    @pytest.fixture
    def db_name(self):
        return 'db_test_sqlalchemy_util'


@pytest.mark.skipif('pymysql is None')
@pytest.mark.usefixtures('mysql_dsn')
class TestDatabaseMySQLWithQuotedName(DatabaseTest):
    @pytest.fixture
    def db_name(self):
        return 'db_test_sqlalchemy-util'


@pytest.mark.usefixtures('postgresql_dsn')
class TestDatabasePostgres(DatabaseTest):
    @pytest.fixture
    def db_name(self):
        return 'db_test_sqlalchemy_util'

    def test_template(self, postgresql_db_user, db_name):
        (
            flexmock(sa.engine.Engine)
            .should_receive('execute')
            .with_args(
                "CREATE DATABASE db_test_sqlalchemy_util ENCODING 'utf8' "
                "TEMPLATE my_template"
            )
        )
        dsn = 'postgresql://{0}@localhost'.format(postgresql_db_user)
        engine = sa.create_engine(dsn)
        create_database(engine, db_name, template='my_template')


class TestDatabasePostgresPg8000(DatabaseTest):
    @pytest.fixture
    def dsn(self, postgresql_db_user):
        return 'postgresql+pg8000://{0}@localhost/{1}'.format(
            postgresql_db_user,
            'db_to_test_create_and_drop_via_pg8000_driver'
        )


class TestDatabasePostgresPsycoPG2CFFI(DatabaseTest):
    @pytest.fixture
    def db_name(self):
        return 'db_to_test_create_and_drop_via_psycopg2cffi_driver'

    @pytest.fixture
    def dsn(self, postgresql_db_user):
        return 'postgresql+psycopg2cffi://{0}@localhost'.format(
            postgresql_db_user
        )


@pytest.mark.usefixtures('postgresql_dsn')
class TestDatabasePostgresWithQuotedName(DatabaseTest):
    @pytest.fixture
    def db_name(self):
        return 'db_test_sqlalchemy-util'

    def test_template(self, postgresql_db_user, db_name):
        (
            flexmock(sa.engine.Engine)
            .should_receive('execute')
            .with_args(
                '''CREATE DATABASE "db_test_sqlalchemy-util"'''
                " ENCODING 'utf8' "
                'TEMPLATE "my-template"'
            )
        )
        dsn = 'postgresql://{0}@localhost'.format(
            postgresql_db_user
        )
        engine = sa.create_engine(dsn)
        create_database(engine, db_name, template='my-template')


class TestDatabasePostgresCreateDatabaseCloseConnection(object):
    def test_create_database_twice(self, postgresql_db_user):
        dsn = 'postgresql://{0}@localhost'.format(
            postgresql_db_user
        )
        databases = [
            'db_test_sqlalchemy-util-a'
            'db_test_sqlalchemy-util-b'
        ]
        engine = sa.create_engine(dsn)
        for db_name in databases:
            assert not database_exists(engine, db_name)
            create_database(engine, db_name, template='template1')
            assert database_exists(engine, db_name)
        for db_name in databases:
            drop_database(engine, db_name)
            assert not database_exists(engine, db_name)


@pytest.mark.usefixtures('mssql_dsn')
class TestDatabaseMssql(DatabaseTest):
    @pytest.fixture
    def db_name(self):
        pytest.importorskip('pyodbc')
        return 'db_test_sqlalchemy_util'
