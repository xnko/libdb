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

#include "db_mysql.h"

int db_mysql_session_start(api_loop_t* loop, db_engine_t* engine, db_mysql_session_t** session)
{
    int length = 0;
    api_pool_t* pool = api_pool_default(loop);
    db_mysql_session_t* mysql_session = (db_mysql_session_t*)api_calloc(pool, sizeof(*mysql_session));
    db_mysql_connection_t* connection = 0;
    db_iface_t* iface = &mysql_session->base.iface;
    int error;

    /*
     * At least keep one connection alive per session
     */
    mysql_session->base.pool.pool_size = engine->pool_size;
    if (mysql_session->base.pool.pool_size == 0)
        mysql_session->base.pool.pool_size = 1;

    /*
     * Copy options
     */

    mysql_session->base.connect_timeout = engine->connect_timeout;
    mysql_session->base.timeout = engine->timeout;

    length = strlen(engine->db.mysql.server);
    if (length > 0)
    {
        mysql_session->ip = (char*)api_alloc(pool, length + 1);
        strcpy(mysql_session->ip, engine->db.mysql.server);
    }

    mysql_session->port = engine->db.mysql.port;

    length = strlen(engine->db.mysql.username);
    if (length > 0)
    {
        mysql_session->username = (char*)api_alloc(pool, length + 1);
        strcpy(mysql_session->username, engine->db.mysql.username);
    }

    length = strlen(engine->db.mysql.password);
    if (length > 0)
    {
        mysql_session->password = (char*)api_alloc(pool, length + 1);
        strcpy(mysql_session->password, engine->db.mysql.password);
    }

    length = strlen(engine->db.mysql.schema);
    if (length > 0)
    {
        mysql_session->schema = (char*)api_alloc(pool, length + 1);
        strcpy(mysql_session->schema, engine->db.mysql.schema);
    }

    mysql_session->base.loop = loop;

    /*
     * Build interface
     */

    iface->session.error = (db_session_error_fn)db_mysql_session_error;
    iface->session.close = (db_session_close_fn)db_mysql_session_close;

    iface->connection.create = (db_connection_create_fn)db_mysql_connection_create;
    iface->connection.open = (db_connection_open_fn)db_mysql_connection_open;
    iface->connection.error = (db_connection_error_fn)db_mysql_connection_error;
    iface->connection.query = (db_connection_query_fn)db_mysql_connection_query;
    iface->connection.affected = (db_connection_affected_fn)db_mysql_connection_affected;
    iface->connection.insert_id = (db_connection_insert_id_fn)db_mysql_connection_insert_id;
    iface->connection.begin = (db_connection_begin_fn)db_mysql_connection_begin;
    iface->connection.commit = (db_connection_commit_fn)db_mysql_connection_commit;
    iface->connection.rollback = (db_connection_rollback_fn)db_mysql_connection_rollback;
    iface->connection.close = (db_connection_close_fn)db_mysql_connection_close;
    iface->connection.destroy = (db_connection_destroy_fn)db_mysql_connection_destroy;

    iface->statement.prepare = (db_statement_prepare_fn)db_mysql_statement_prepare;
    iface->statement.reset = (db_statement_reset_fn)db_mysql_statement_reset;
    iface->statement.bind_null = (db_statement_bind_null_fn)db_mysql_statement_bind_null;
    iface->statement.bind_bool = (db_statement_bind_bool_fn)db_mysql_statement_bind_bool;
    iface->statement.bind_byte = (db_statement_bind_byte_fn)db_mysql_statement_bind_byte;
    iface->statement.bind_short = (db_statement_bind_short_fn)db_mysql_statement_bind_short;
    iface->statement.bind_int = (db_statement_bind_int_fn)db_mysql_statement_bind_int;
    iface->statement.bind_int64 = (db_statement_bind_int64_fn)db_mysql_statement_bind_int64;
    iface->statement.bind_float = (db_statement_bind_float_fn)db_mysql_statement_bind_float;
    iface->statement.bind_double = (db_statement_bind_double_fn)db_mysql_statement_bind_double;
    iface->statement.bind_time = (db_statement_bind_time_fn)db_mysql_statement_bind_time;
    iface->statement.bind_date = (db_statement_bind_date_fn)db_mysql_statement_bind_date;
    iface->statement.bind_datetime = (db_statement_bind_datetime_fn)db_mysql_statement_bind_datetime;
    iface->statement.bind_timestamp = (db_statement_bind_timestamp_fn)db_mysql_statement_bind_timestamp;
    iface->statement.bind_string = (db_statement_bind_string_fn)db_mysql_statement_bind_string;
    iface->statement.bind_binary = (db_statement_bind_binary_fn)db_mysql_statement_bind_binary;
    iface->statement.bind_blob = (db_statement_bind_blob_fn)db_mysql_statement_bind_blob;
    iface->statement.exec = (db_statement_exec_fn)db_mysql_statement_exec;
    iface->statement.close = (db_statement_close_fn)db_mysql_statement_close;

    iface->result.fetch_columns = (db_result_fetch_columns_fn)db_mysql_result_fetch_columns;
    iface->result.fetch_rows = (db_result_fetch_rows_fn)db_mysql_result_fetch_rows;
    iface->result.close = (db_result_close_fn)db_mysql_result_close;

    *session = mysql_session;

    /*
     * Create and cache first connection.
     * Also check server availability and auth status
     */

    error = db_pool_open_connection((db_session_t*)mysql_session, (db_connection_t**)&connection);
    if (DB_OK == error)
    {
        /*
         * Cache connection
         */
        db_pool_close_connection((db_connection_t*)connection);
    }
    else
    {
        /*
         * On failure free memory
         */
        if (connection)
            mysql_session->base.iface.connection.destroy((db_connection_t*)connection);
    }

    return error;
}

int db_mysql_session_error(db_mysql_session_t* session, db_error_t* error)
{
    memcpy(error, &session->base.error, sizeof(*error));
}

int db_mysql_session_close(db_mysql_session_t* session)
{
    api_pool_t* pool = api_pool_default(session->base.loop);

    db_pool_destroy((db_session_t*)session);

    if (session->ip)
        api_free(pool, strlen(session->ip) + 1, session->ip);

    if (session->username)
        api_free(pool, strlen(session->username) + 1, session->username);

    if (session->password)
        api_free(pool, strlen(session->password) + 1, session->password);

    if (session->schema)
        api_free(pool, strlen(session->schema) + 1, session->schema);

    db_error_cleanup(pool, &session->base.error);
    api_free(pool, sizeof(*session), session);

    return DB_OK;
}
