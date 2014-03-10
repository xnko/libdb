#include <memory.h>
#include "db_odbc.h"

static db_impl_t odbc_impl;

int db_session_odbc_cleanup(db_session_t* session)
{
	return 1;
}

int db_odbc_connection_create(db_session_t* session, db_connection_t* connection);
int db_odbc_connection_open(db_session_t* session, db_connection_t* connection);
int db_odbc_connection_begin(db_connection_t* connection);
int db_odbc_connection_commit(db_connection_t* connection);
int db_odbc_connection_rollback(db_connection_t* connection);
int db_odbc_connection_close(db_connection_t* connection);
int db_odbc_connection_destroy(db_connection_t* connection);

int db_session_odbc_init(db_session_t* session, const char* filename, int flags)
{
	session->type = DB_BACKEND_SQLITE3;
	session->worker = &worker;
	session->impl = &odbc_impl;

	odbc_impl.session.cleanup = db_session_odbc_cleanup;

	odbc_impl.connection.create = db_odbc_connection_create;
	odbc_impl.connection.open = db_odbc_connection_open;
	odbc_impl.connection.begin = db_odbc_connection_begin;
	odbc_impl.connection.commit = db_odbc_connection_commit;
	odbc_impl.connection.rollback = db_odbc_connection_rollback;
	odbc_impl.connection.close = db_odbc_connection_close;
	odbc_impl.connection.destroy = db_odbc_connection_destroy;

	return 1;
}

int db_odbc_connection_create(db_session_t* session, db_connection_t* connection)
{
	bzero(connection, sizeof(*connection));

	connection->session = session;

	return 1;
}

int db_odbc_connection_open(db_session_t* session, db_connection_t* connection)
{
	bzero(connection, sizeof(*connection));

	connection->session = session;

	return 1;
}

int db_odbc_connection_begin(db_connection_t* connection)
{
	return 1;
}

int db_odbc_connection_commit(db_connection_t* connection)
{
	return 1;
}

int db_odbc_connection_rollback(db_connection_t* connection)
{
	return 1;
}

int db_odbc_connection_close(db_connection_t* connection)
{
	bzero(connection, sizeof(*connection));
}

int db_odbc_connection_destroy(db_connection_t* connection)
{
	bzero(connection, sizeof(*connection));
}