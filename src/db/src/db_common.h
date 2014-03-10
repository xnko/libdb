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

#ifndef DB_COMMON_H_INCLUDED
#define DB_COMMON_H_INCLUDED

#include "../include/db.h"
#include "api_list.h"

typedef int (*db_session_error_fn)(db_session_t* session, db_error_t* error);
typedef int (*db_session_close_fn)(db_session_t* session);

typedef int (*db_connection_create_fn)(db_session_t* session, db_connection_t** connection);
typedef int (*db_connection_open_fn)(db_session_t* session, db_connection_t** connection);
typedef int (*db_connection_error_fn)(db_connection_t* connection, db_error_t* error);
typedef int (*db_connection_query_fn)(db_connection_t* connection, const char* sql, db_result_t** result);
typedef int (*db_connection_affected_fn)(db_connection_t* connection, uint64_t* affected);
typedef int (*db_connection_insert_id_fn)(db_connection_t* connection, uint64_t* insert_id);
typedef int (*db_connection_begin_fn)(db_connection_t* connection);
typedef int (*db_connection_commit_fn)(db_connection_t* connection);
typedef int (*db_connection_rollback_fn)(db_connection_t* connection);
typedef int (*db_connection_close_fn)(db_connection_t* connection);
typedef int (*db_connection_destroy_fn)(db_connection_t* connection);

typedef int (*db_statement_prepare_fn)(db_connection_t* connection, const char* sql, db_statement_t** statement);
typedef int (*db_statement_reset_fn)(db_statement_t* statement);
typedef int (*db_statement_bind_null_fn)(db_statement_t* statement, int index);
typedef int (*db_statement_bind_bool_fn)(db_statement_t* statement, int index, char value);
typedef int (*db_statement_bind_byte_fn)(db_statement_t* statement, int index, char value);
typedef int (*db_statement_bind_short_fn)(db_statement_t* statement, int index, short value);
typedef int (*db_statement_bind_int_fn)(db_statement_t* statement, int index, int value);
typedef int (*db_statement_bind_int64_fn)(db_statement_t* statement, int index, int64_t value);
typedef int (*db_statement_bind_float_fn)(db_statement_t* statement, int index, float value);
typedef int (*db_statement_bind_double_fn)(db_statement_t* statement, int index, double value);
typedef int (*db_statement_bind_time_fn)(db_statement_t* statement, int index, db_time_t* value);
typedef int (*db_statement_bind_date_fn)(db_statement_t* statement, int index, db_date_t* value);
typedef int (*db_statement_bind_datetime_fn)(db_statement_t* statement, int index, db_date_t* value);
typedef int (*db_statement_bind_timestamp_fn)(db_statement_t* statement, int index, db_date_t* value);
typedef int (*db_statement_bind_string_fn)(db_statement_t* statement, int index, const char* value);
typedef int (*db_statement_bind_binary_fn)(db_statement_t* statement, int index, void* value, uint64_t size);
typedef int (*db_statement_bind_blob_fn)(db_statement_t* statement, int index, void* value, uint64_t size);
typedef int (*db_statement_exec_fn)(db_statement_t* statement, db_result_t** result);
typedef int (*db_statement_close_fn)(db_statement_t* statement);

typedef int (*db_result_fetch_columns_fn)(db_result_t* result, db_column_t** columns, int* num_columns);
typedef int (*db_result_fetch_rows_fn)(db_result_t* result, db_value_t*** rows, int* count);
typedef int (*db_result_close_fn)(db_result_t* result);

typedef struct db_iface_t {
	struct {
        db_session_error_fn error;
		db_session_close_fn close;
	} session;
	struct {
        db_connection_create_fn create;
        db_connection_open_fn open;
        db_connection_error_fn error;
        db_connection_query_fn query;
        db_connection_affected_fn affected;
        db_connection_insert_id_fn insert_id;
        db_connection_begin_fn begin;
        db_connection_commit_fn commit;
        db_connection_rollback_fn rollback;
        db_connection_close_fn close;
        db_connection_destroy_fn destroy;
	} connection;
	struct {
		db_statement_prepare_fn prepare;
		db_statement_reset_fn reset;
		db_statement_bind_null_fn bind_null;
        db_statement_bind_bool_fn bind_bool;
        db_statement_bind_byte_fn bind_byte;
        db_statement_bind_short_fn bind_short;
		db_statement_bind_int_fn bind_int;
		db_statement_bind_int64_fn bind_int64;
		db_statement_bind_float_fn bind_float;
		db_statement_bind_double_fn bind_double;
		db_statement_bind_time_fn bind_time;
		db_statement_bind_date_fn bind_date;
		db_statement_bind_datetime_fn bind_datetime;
		db_statement_bind_timestamp_fn bind_timestamp;
		db_statement_bind_string_fn bind_string;
		db_statement_bind_binary_fn bind_binary;
		db_statement_bind_blob_fn bind_blob;
		db_statement_exec_fn exec;
		db_statement_close_fn close;
	} statement;
	struct {
        db_result_fetch_columns_fn fetch_columns;
        db_result_fetch_rows_fn fetch_rows;
        db_result_close_fn close;
	} result;
} db_iface_t;

typedef struct db_pool_t {
    struct {
        db_connection_t* head;
        db_connection_t* tail;
    } free_list;
    int free_size;
    int pool_size;
} db_pool_t;

typedef struct db_session_t {
	db_iface_t iface;
    db_error_t error;
	api_loop_t* loop;
    db_pool_t pool;
    uint64_t connect_timeout;
    uint64_t timeout;
} db_session_t;

typedef struct db_connection_t {
    db_connection_t* next;
    db_connection_t* prev;
	db_session_t* session;
    db_error_t error;
    db_result_t* result;
} db_connection_t;

typedef struct db_statement_t {
	db_connection_t* connection;
} db_statement_t;

typedef struct db_result_t {
    db_connection_t* connection;
} db_result_t;

void db_error_override(api_pool_t* pool, db_error_t* dst, db_error_t* src);
void db_error_cleanup(api_pool_t* pool, db_error_t* error);

int db_pool_open_connection(db_session_t* session, db_connection_t** connection);
int db_pool_close_connection(db_connection_t* connection);
int db_pool_destroy(db_session_t* session);

#endif // DB_COMMON_H_INCLUDED