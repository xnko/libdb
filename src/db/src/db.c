/* Copyright (c) 2014, Artak Khnkoyan <artak.khnkoyan@gmail.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */

#include "db_common.h"
#include "mysql/db_mysql.h"

int db_session_start(api_loop_t* loop, db_engine_t* engine, db_session_t** session)
{
    switch (engine->type) {
    case DB_ENGINE_MYSQL:
        return db_mysql_session_start(loop, engine, (db_mysql_session_t**)session);
    };

    return DB_NOT_SUPPORTED;
}

int db_session_error(db_session_t* session, db_error_t* error)
{
    return session->iface.session.error(session, error);
}

int db_session_close(db_session_t* session)
{
    return session->iface.session.close(session);
}

int db_connection_open(db_session_t* session, db_connection_t** connection)
{
    return session->iface.connection.open(session, connection);
}

int db_connection_error(db_connection_t* connection, db_error_t* error)
{
    return connection->session->iface.connection.error(connection, error);
}

int db_connection_query(db_connection_t* connection, const char* sql, db_result_t** result)
{
    return connection->session->iface.connection.query(connection, sql, result);
}

int db_connection_affected(db_connection_t* connection, uint64_t* affected)
{
    return connection->session->iface.connection.affected(connection, affected);
}

int db_connection_insert_id(db_connection_t* connection, uint64_t* insert_id)
{
    return connection->session->iface.connection.insert_id(connection, insert_id);
}

int db_connection_begin(db_connection_t* connection)
{
    return connection->session->iface.connection.begin(connection);
}

int db_connection_commit(db_connection_t* connection)
{
    return connection->session->iface.connection.commit(connection);
}

int db_connection_rollback(db_connection_t* connection)
{
    return connection->session->iface.connection.rollback(connection);
}

int db_connection_close(db_connection_t* connection)
{
    return connection->session->iface.connection.close(connection);
}

int db_statement_prepare(db_connection_t* connection, const char* sql, db_statement_t** statement)
{
    return connection->session->iface.statement.prepare(connection, sql, statement);
}

int db_statement_bind_null(db_statement_t* statement, int index)
{
    return statement->connection->session->iface.statement.bind_null(statement, index);
}

int db_statement_bind_bool(db_statement_t* statement, int index, char value)
{
    return statement->connection->session->iface.statement.bind_bool(statement, index, value);
}

int db_statement_bind_byte(db_statement_t* statement, int index, char value)
{
    return statement->connection->session->iface.statement.bind_byte(statement, index, value);
}

int db_statement_bind_short(db_statement_t* statement, int index, short value)
{
    return statement->connection->session->iface.statement.bind_short(statement, index, value);
}

int db_statement_bind_int(db_statement_t* statement, int index, int value)
{
    return statement->connection->session->iface.statement.bind_int(statement, index, value);
}

int db_statement_bind_int64(db_statement_t* statement, int index, int64_t value)
{
    return statement->connection->session->iface.statement.bind_int64(statement, index, value);
}

int db_statement_bind_float(db_statement_t* statement, int index, float value)
{
    return statement->connection->session->iface.statement.bind_float(statement, index, value);
}

int db_statement_bind_double(db_statement_t* statement, int index, double value)
{
    return statement->connection->session->iface.statement.bind_double(statement, index, value);
}

int db_statement_bind_time(db_statement_t* statement, int index, db_time_t* value)
{
    return statement->connection->session->iface.statement.bind_time(statement, index, value);
}

int db_statement_bind_date(db_statement_t* statement, int index, db_date_t* value)
{
    return statement->connection->session->iface.statement.bind_date(statement, index, value);
}

int db_statement_bind_datetime(db_statement_t* statement, int index, db_date_t* value)
{
    return statement->connection->session->iface.statement.bind_datetime(statement, index, value);
}

int db_statement_bind_timestamp(db_statement_t* statement, int index, db_date_t* value)
{
    return statement->connection->session->iface.statement.bind_timestamp(statement, index, value);
}

int db_statement_bind_string(db_statement_t* statement, int index, const char* value)
{
    return statement->connection->session->iface.statement.bind_string(statement, index, value);
}

int db_statement_bind_binary(db_statement_t* statement, int index, void* value, uint64_t size)
{
    return statement->connection->session->iface.statement.bind_binary(statement, index, value, size);
}

int db_statement_bind_blob(db_statement_t* statement, int index, void* value, uint64_t size)
{
    return statement->connection->session->iface.statement.bind_blob(statement, index, value, size);
}

int db_statement_exec(db_statement_t* statement, db_result_t** result)
{
    return statement->connection->session->iface.statement.exec(statement, result);
}

int db_statement_close(db_statement_t* statement)
{
    return statement->connection->session->iface.statement.close(statement);
}

int db_result_fetch_columns(db_result_t* result, db_column_t** columns, int* num_columns)
{
    return result->connection->session->iface.result.fetch_columns(result, columns, num_columns);
}

int db_result_fetch_rows(db_result_t* result, db_value_t*** rows, int* count)
{
    return result->connection->session->iface.result.fetch_rows(result, rows, count);
}

int db_result_close(db_result_t* result)
{
    return result->connection->session->iface.result.close(result);
}

/*
 * Connection pooling
 */

int db_pool_open_connection(db_session_t* session, db_connection_t** connection)
{
    if (session->pool.free_size > 0)
    {
        *connection = (db_connection_t*)api_list_pop_head((api_list_t*)&session->pool.free_list);
        --session->pool.free_size;

        return DB_OK;
    }

    return session->iface.connection.create(session, connection);
}

int db_pool_close_connection(db_connection_t* connection)
{
    if (connection->session->pool.free_size < connection->session->pool.pool_size)
    {
        api_list_push_head((api_list_t*)&connection->session->pool.free_list, (api_node_t*)connection);
        ++connection->session->pool.free_size;

        return DB_OK;
    }

    return connection->session->iface.connection.destroy(connection);
}

int db_pool_destroy(db_session_t* session)
{
    db_connection_t* connection = (db_connection_t*)api_list_pop_head((api_list_t*)&session->pool.free_list);
    while (connection)
    {
        connection->session->iface.connection.destroy(connection);
        connection = (db_connection_t*)api_list_pop_head((api_list_t*)&session->pool.free_list);
    }

    return DB_OK;
}