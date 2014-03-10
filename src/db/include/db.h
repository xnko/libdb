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

#ifndef DB_H_INCLUDED
#define DB_H_INCLUDED

#ifdef __cplusplus
extern "C" {
#endif

#ifdef _WIN32
typedef signed __int64 int64_t;
#else
#include <sys/types.h>
#endif

#include "../../api/include/api.h"

#ifdef _WIN32
#if defined(BUILD_DB_SHARED)
    #define DB_EXTERN __declspec(dllexport)
#elif defined(USE_DB_SHARED)
    #define DB_EXTERN __declspec(dllimport)
#else
    #define DB_EXTERN
#endif
#elif __GNUC__ >= 4
    #define DB_EXTERN __attribute__((visibility("default")))
#else
    #define DB_EXTERN
#endif

#define DB_ENGINE_MYSQL         1
#define DB_ENGINE_DB2           2
#define DB_ENGINE_FIREBIRD      3
#define DB_ENGINE_SQLITE3       4
#define DB_ENGINE_POSTGRESQL    5
#define DB_ENGINE_ORACLE        6
#define DB_ENGINE_TDS           7

#define DB_OK               0
#define DB_FAILED           1
#define DB_UNKNOWN          2
#define DB_CONNECT_FAILED   3
#define DB_UNAVAILABLE      4
#define DB_NOT_SUPPORTED    5
#define DB_OUT_OF_INDEX     6
#define DB_MISMATCH         7
#define DB_TOO_LONG         8
#define DB_OUT_OF_SYNC      9
#define DB_NO_DATA          10

#define DB_TYPE_BOOL        1
#define DB_TYPE_BYTE        2
#define DB_TYPE_SHORT       3
#define DB_TYPE_INT         4
#define DB_TYPE_INT64       5
#define DB_TYPE_FLOAT       6
#define DB_TYPE_DOUBLE      7
#define DB_TYPE_TIME        9
#define DB_TYPE_DATE        8
#define DB_TYPE_DATETIME    10
#define DB_TYPE_TIMESTAMP   11
#define DB_TYPE_STRING      12
#define DB_TYPE_BINARY      13

typedef struct db_engine_t {
    int type;
    uint64_t connect_timeout;
    uint64_t timeout;
    int pool_size;
    union {
        struct {
            const char* filename;
            int flags;
        } sqlite3;
        struct {
            const char* username;
            const char* password;
            const char* schema;
            const char* server;
            int port;
            int flags;
        } mysql;
    } db;
} db_engine_t;

typedef struct db_date_t {
    short year;
    unsigned char month;
    unsigned char day;
    unsigned char hour;
    unsigned char minute;
    unsigned char second;
    int microsecond;
} db_date_t;

typedef struct db_time_t {
    unsigned char is_negative;
    int days;
    unsigned char hours;
    unsigned char minutes;
    unsigned char seconds;
    int microseconds;
} db_time_t;

typedef struct db_error_t {
    int  code;     // engine specific error code
    char state[6]; // ANSI SQLSTATE code, see http://raymondkolbe.com/2009/03/08/sql-92-sqlstate-codes/
    char* message; // human readable error message
} db_error_t;

typedef struct db_column_t {
    char*    name;
    int      type;   // DB_TYPE_*
    uint64_t length; // max storage size in bytes, 0 if not specified
} db_column_t;

typedef struct db_value_t {
    uint64_t size;
    int is_null;
    union {
        char value_bool;
        char value_byte;
        short value_short;
        int value_int;
        int64_t value_int64;
        float value_float;
        double value_double;
        db_time_t value_time;
        db_date_t value_date;
        db_date_t value_datetime;
        db_date_t value_timestamp;
        char* value_string;
        void* value_binary;
    };
} db_value_t;

typedef struct db_session_t db_session_t;
typedef struct db_connection_t db_connection_t;
typedef struct db_statement_t db_statement_t;
typedef struct db_result_t db_result_t;

DB_EXTERN int db_session_start(api_loop_t* loop, db_engine_t* engine, db_session_t** session);
DB_EXTERN int db_session_error(db_session_t* session, db_error_t* error);
DB_EXTERN int db_session_close(db_session_t* session);

DB_EXTERN int db_connection_open(db_session_t* session, db_connection_t** connection);
DB_EXTERN int db_connection_error(db_connection_t* connection, db_error_t* error);
DB_EXTERN int db_connection_query(db_connection_t* connection, const char* sql, db_result_t** result);
DB_EXTERN int db_connection_affected(db_connection_t* connection, uint64_t* affected);
DB_EXTERN int db_connection_insert_id(db_connection_t* connection, uint64_t* insert_id);
DB_EXTERN int db_connection_begin(db_connection_t* connection);
DB_EXTERN int db_connection_commit(db_connection_t* connection);
DB_EXTERN int db_connection_rollback(db_connection_t* connection);
DB_EXTERN int db_connection_close(db_connection_t* connection);

DB_EXTERN int db_statement_prepare(db_connection_t* connection, const char* sql, db_statement_t** statement);
DB_EXTERN int db_statement_reset(db_statement_t* statement);
DB_EXTERN int db_statement_bind_null(db_statement_t* statement, int index);
DB_EXTERN int db_statement_bind_bool(db_statement_t* statement, int index, char value);
DB_EXTERN int db_statement_bind_byte(db_statement_t* statement, int index, char value);
DB_EXTERN int db_statement_bind_short(db_statement_t* statement, int index, short value);
DB_EXTERN int db_statement_bind_int(db_statement_t* statement, int index, int value);
DB_EXTERN int db_statement_bind_int64(db_statement_t* statement, int index, int64_t value);
DB_EXTERN int db_statement_bind_float(db_statement_t* statement, int index, float value);
DB_EXTERN int db_statement_bind_double(db_statement_t* statement, int index, double value);
DB_EXTERN int db_statement_bind_time(db_statement_t* statement, int index, db_time_t* value);
DB_EXTERN int db_statement_bind_date(db_statement_t* statement, int index, db_date_t* value);
DB_EXTERN int db_statement_bind_datetime(db_statement_t* statement, int index, db_date_t* value);
DB_EXTERN int db_statement_bind_timestamp(db_statement_t* statement, int index, db_date_t* value);
DB_EXTERN int db_statement_bind_string(db_statement_t* statement, int index, const char* value);
DB_EXTERN int db_statement_bind_binary(db_statement_t* statement, int index, void* value, uint64_t size);
DB_EXTERN int db_statement_bind_blob(db_statement_t* statement, int index, void* value, uint64_t size);
DB_EXTERN int db_statement_exec(db_statement_t* statement, db_result_t** result);
DB_EXTERN int db_statement_close(db_statement_t* statement);

DB_EXTERN int db_result_fetch_columns(db_result_t* result, db_column_t** columns, int* num_columns);
DB_EXTERN int db_result_fetch_rows(db_result_t* result, db_value_t*** rows, int* count);
DB_EXTERN int db_result_close(db_result_t* result);

#ifdef __cplusplus
} // extern "C"
#endif

#endif // DB_H_INCLUDED