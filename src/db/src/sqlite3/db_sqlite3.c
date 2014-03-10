#include <memory.h>
#include <wchar.h>
#include "db_sqlite3.h"

static db_impl_t sqlite3_impl;

int db_session_sqlite3_cleanup(db_session_t* session)
{
	sqlite3_close_v2(session->sqlite3.handle);

	return 1;
}

int db_sqlite3_connection_create(db_session_t* session, db_connection_t* connection);
int db_sqlite3_connection_open(db_session_t* session, db_connection_t* connection);
int db_sqlite3_connection_begin(db_connection_t* connection);
int db_sqlite3_connection_commit(db_connection_t* connection);
int db_sqlite3_connection_rollback(db_connection_t* connection);
int db_sqlite3_connection_close(db_connection_t* connection);
int db_sqlite3_connection_destroy(db_connection_t* connection);

int db_sqlite3_statement_init(db_connection_t* connection, db_statement_t* statement, const char* sql);
int db_sqlite3_statement_bind_null(db_statement_t* statement, int index);
int db_sqlite3_statement_bind_int(db_statement_t* statement, int index, int value);
int db_sqlite3_statement_bind_int64(db_statement_t* statement, int index, int64_t value);
int db_sqlite3_statement_bind_double(db_statement_t* statement, int index, double value);
int db_sqlite3_statement_bind_date(db_statement_t* statement, int index, time_t value);
int db_sqlite3_statement_bind_time(db_statement_t* statement, int index, time_t value);
int db_sqlite3_statement_bind_timespan(db_statement_t* statement, int index, time_t value);
int db_sqlite3_statement_bind_string(db_statement_t* statement, int index, const char* value);
int db_sqlite3_statement_bind_wstring(db_statement_t* statement, int index, const wchar_t* value);
int db_sqlite3_statement_exec(db_statement_t* statement, db_result_t* result);
int db_sqlite3_statement_cleanup(db_statement_t* statement);

int db_session_sqlite3_init(db_session_t* session, const char* filename, int flags)
{
	session->type = DB_BACKEND_SQLITE3;
	session->worker = &worker;
	session->impl = &sqlite3_impl;

	sqlite3_open_v2(filename, &session->sqlite3.handle, flags, 0);

	sqlite3_impl.session.cleanup = db_session_sqlite3_cleanup;

	sqlite3_impl.connection.create = db_sqlite3_connection_create;
	sqlite3_impl.connection.open = db_sqlite3_connection_open;
	sqlite3_impl.connection.begin = db_sqlite3_connection_begin;
	sqlite3_impl.connection.commit = db_sqlite3_connection_commit;
	sqlite3_impl.connection.rollback = db_sqlite3_connection_rollback;
	sqlite3_impl.connection.close = db_sqlite3_connection_close;
	sqlite3_impl.connection.destroy = db_sqlite3_connection_destroy;

	sqlite3_impl.statement.init = db_sqlite3_statement_init;
	sqlite3_impl.statement.bind_null = db_sqlite3_statement_bind_null;
	sqlite3_impl.statement.bind_int = db_sqlite3_statement_bind_int;
	sqlite3_impl.statement.bind_int64 = db_sqlite3_statement_bind_int64;
	sqlite3_impl.statement.bind_double = db_sqlite3_statement_bind_double;
	sqlite3_impl.statement.bind_date = db_sqlite3_statement_bind_date;
	sqlite3_impl.statement.bind_time = db_sqlite3_statement_bind_time;
	sqlite3_impl.statement.bind_timespan = db_sqlite3_statement_bind_timespan;
	sqlite3_impl.statement.bind_string = db_sqlite3_statement_bind_string;
	sqlite3_impl.statement.bind_wstring = db_sqlite3_statement_bind_wstring;
	sqlite3_impl.statement.exec = db_sqlite3_statement_exec;
	sqlite3_impl.statement.cleanup = db_sqlite3_statement_cleanup;

	return 1;
}

int db_sqlite3_connection_create(db_session_t* session, db_connection_t* connection)
{
	bzero(connection, sizeof(*connection));

	connection->session = session;

	return 1;
}

int db_sqlite3_connection_open(db_session_t* session, db_connection_t* connection)
{
	bzero(connection, sizeof(*connection));

	connection->session = session;

	return 1;
}

int db_sqlite3_connection_begin(db_connection_t* connection)
{
	sqlite3_exec(connection->session->sqlite3.handle, "Begin;", 0, 0, 0);
	return 1;
}

int db_sqlite3_connection_commit(db_connection_t* connection)
{
	sqlite3_exec(connection->session->sqlite3.handle, "Commit;", 0, 0, 0);
	return 1;
}

int db_sqlite3_connection_rollback(db_connection_t* connection)
{
	sqlite3_exec(connection->session->sqlite3.handle, "Rollback;", 0, 0, 0);
	return 1;
}

int db_sqlite3_connection_close(db_connection_t* connection)
{
	bzero(connection, sizeof(*connection));
}

int db_sqlite3_connection_destroy(db_connection_t* connection)
{
	bzero(connection, sizeof(*connection));
}

int db_sqlite3_statement_init(db_connection_t* connection, db_statement_t* statement, const char* sql)
{
	bzero(statement, sizeof(statement));

	sqlite3_prepare_v2(connection->session->sqlite3.handle, sql, -1, &statement->sqlite3.stmt, 0);

	return 1;
}

int db_sqlite3_statement_bind_null(db_statement_t* statement, int index)
{
	sqlite3_bind_null(statement->sqlite3.stmt, index);

	return 1;
}

int db_sqlite3_statement_bind_int(db_statement_t* statement, int index, int value)
{
	sqlite3_bind_int(statement->sqlite3.stmt, index, value);

	return 1;
}

int db_sqlite3_statement_bind_int64(db_statement_t* statement, int index, int64_t value)
{
	sqlite3_bind_int64(statement->sqlite3.stmt, index, value);

	return 1;
}

int db_sqlite3_statement_bind_double(db_statement_t* statement, int index, double value)
{
	sqlite3_bind_double(statement->sqlite3.stmt, index, value);

	return 1;
}

int db_sqlite3_statement_bind_date(db_statement_t* statement, int index, time_t value);
int db_sqlite3_statement_bind_time(db_statement_t* statement, int index, time_t value);
int db_sqlite3_statement_bind_timespan(db_statement_t* statement, int index, time_t value);
int db_sqlite3_statement_bind_string(db_statement_t* statement, int index, const char* value)
{
	sqlite3_bind_text(statement->sqlite3.stmt, index, value, strlen(value));

	return 1;
}

int db_sqlite3_statement_bind_wstring(db_statement_t* statement, int index, const wchar_t* value)
{
	sqlite3_bind_text16(statement->sqlite3.stmt, index, value, 2 * wcslen(value), 0);

	return 1;
}

int db_sqlite3_statement_exec(db_statement_t* statement, db_result_t* result);

int db_sqlite3_statement_cleanup(db_statement_t* statement)
{
	sqlite3_finalize(statement->sqlite3.stmt);
	return 1;
}
