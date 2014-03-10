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

#ifndef DB_MYSQL_H_INCLUDED
#define DB_MYSQL_H_INCLUDED

#include "../db_common.h"

/*
 * Constants
 */

#define CLIENT_PLUGIN_AUTH  (1UL << 19)
#define CLIENT_SECURE_CONNECTION 32768

#define CLIENT_LONG_PASSWORD    1	/* new more secure passwords */
#define CLIENT_FOUND_ROWS       2	/* Found instead of affected rows */
#define CLIENT_LONG_FLAG        4	/* Get all column flags */
#define CLIENT_CONNECT_WITH_DB  8	/* One can specify db on connect */
#define CLIENT_NO_SCHEMA        16	/* Don't allow database.table.column */
#define CLIENT_COMPRESS         32	/* Can use compression protocol */
#define CLIENT_ODBC             64	/* Odbc client */
#define CLIENT_LOCAL_FILES      128	/* Can use LOAD DATA LOCAL */
#define CLIENT_IGNORE_SPACE     256	/* Ignore spaces before '(' */
#define CLIENT_PROTOCOL_41      512	/* New 4.1 protocol */
#define CLIENT_INTERACTIVE      1024	/* This is an interactive client */
#define CLIENT_SSL              2048	/* Switch to SSL after handshake */
#define CLIENT_IGNORE_SIGPIPE   4096    /* IGNORE sigpipes */
#define CLIENT_TRANSACTIONS     8192	/* Client knows about transactions */
#define CLIENT_RESERVED         16384   /* Old flag for 4.1 protocol  */
#define CLIENT_SECURE_CONNECTION 32768  /* New 4.1 authentication */
#define CLIENT_MULTI_STATEMENTS (1UL << 16) /* Enable/disable multi-stmt support */
#define CLIENT_MULTI_RESULTS    (1UL << 17) /* Enable/disable multi-results */
#define CLIENT_PS_MULTI_RESULTS (1UL << 18) /* Multi-results in PS-protocol */

// http://my.safaribooksonline.com/0596009577/orm9780596009571-chp-4-sect-5

#define COM_QUIT            1  /* End the session */
#define COM_INIT_DB         2  /* Change the default database for the session */
#define COM_QUERY           3  /* Run the query */
#define COM_REFRESH         7  /* Refresh server */
#define COM_SHUTDOWN        8  /* Shutdown the server */
#define COM_STATISTICS      9  /* Get server statistics */
#define COM_DEBUG           13 /* Dump debug information into error log */
#define COM_PING            14 /* Check server reachability */
#define COM_CHANGE_USER     17 /* Change session user */
#define COM_BINLOG_DUMP     18 /* Replication feed */
#define COM_TABLE_DUMP      19 /* Get table raw data */
#define COM_REGISTER_SLAVE  21 /* May become obsolete */
#define COM_STMT_PREPARE    22 /* Prepare statement */
#define COM_STMT_EXECUTE    23 /* Execute statement */
#define COM_STMT_LONG_DATA  24 /* Bind long data */
#define COM_STMT_CLOSE      25 /* Close statement */
#define COM_STMT_RESET      26 /* Reset statement */
#define COM_SET_OPTION      27 /* Enable/disable multiple statements in query */
#define COM_STMT_FETCH      28 /* Fetcha data from statement */

//http://dev.mysql.com/doc/internals/en/status-flags.html

#define SERVER_STATUS_IN_TRANS              0x0001 /* a transaction is active */
#define SERVER_STATUS_AUTOCOMMIT            0x0002 /* auto-commit is enabled */
#define SERVER_MORE_RESULTS_EXISTS          0x0008 /* multi resultset */
#define SERVER_STATUS_NO_GOOD_INDEX_USED    0x0010
#define SERVER_STATUS_NO_INDEX_USED         0x0020
#define SERVER_STATUS_CURSOR_EXISTS         0x0040 /* COM_STMT_FETCH has to be used to fetch the row-data */
#define SERVER_STATUS_LAST_ROW_SENT         0x0080
#define SERVER_STATUS_DB_DROPPED            0x0100
#define SERVER_STATUS_NO_BACKSLASH_ESCAPES  0x0200
#define SERVER_STATUS_METADATA_CHANGED      0x0400
#define SERVER_QUERY_WAS_SLOW               0x0800
#define SERVER_PS_OUT_PARAMS                0x1000

#define PACKET_OK       0
#define PACKET_EOF      0xfe
#define PACKET_ERROR    0xff

#define MYSQL_TYPE_DECIMAL      0x00    /* lenenc string */
#define MYSQL_TYPE_TINY         0x01    /* 1 byte */
#define MYSQL_TYPE_SHORT        0x02    /* 2 byte */
#define MYSQL_TYPE_LONG         0x03    /* 4 byte */
#define MYSQL_TYPE_FLOAT        0x04    /* 4 byte, same as c float */
#define MYSQL_TYPE_DOUBLE       0x05    /* 8 byte, same as c double */
#define MYSQL_TYPE_NULL         0x06    /* 0 byte, null bitmap only */
#define MYSQL_TYPE_TIMESTAMP    0x07    /* max 12 byte */
#define MYSQL_TYPE_LONGLONG     0x08    /* 8 byte */
#define MYSQL_TYPE_INT24        0x09    /* 4 byte */
#define MYSQL_TYPE_DATE         0x0a    /* max 12 byte */
#define MYSQL_TYPE_TIME         0x0b    /* max 13 byte */
#define MYSQL_TYPE_DATETIME     0x0c    /* max 12 byte */
#define MYSQL_TYPE_YEAR         0x0d    /* 2 byte */
#define MYSQL_TYPE_VARCHAR      0x0f    /* lenenc string */
#define MYSQL_TYPE_BIT          0x10    /* lenenc string */
#define MYSQL_TYPE_NEWDECIMAL   0xf6    /* lenenc string */
#define MYSQL_TYPE_ENUM         0xf7    /* lenenc string */
#define MYSQL_TYPE_SET          0xf8    /* lenenc string */
#define MYSQL_TYPE_TINY_BLOB    0xf9    /* lenenc string */
#define MYSQL_TYPE_MEDIUM_BLOB  0xfa    /* lenenc string */
#define MYSQL_TYPE_LONG_BLOB    0xfb    /* lenenc string */
#define MYSQL_TYPE_BLOB         0xfc    /* lenenc string */
#define MYSQL_TYPE_VAR_STRING   0xfd    /* lenenc string */
#define MYSQL_TYPE_STRING       0xfe    /* lenenc string */
#define MYSQL_TYPE_GEOMETRY     0xff    /* lenenc string */

/*
 * Internal structures
 */

typedef struct db_mysql_packet_t {
    unsigned int size;
    unsigned char sequence;
    char* data;
} db_mysql_packet_t;

typedef struct db_mysql_status_t {
    int code;
    union {
        struct {
            uint64_t affected_rows;
            uint64_t last_insert_id;
            unsigned short flags;
            unsigned short warnings;
            char* info;
        } ok;
        struct {
            unsigned short warnings;
            unsigned short flags;
        } eof;
        db_error_t err;
    };
} db_mysql_status_t;

typedef struct db_mysql_session_t {
    db_session_t base;
    char* username;
    char* password;
    char* schema;
    char* ip;
    int port;
    int flags;
    struct {
        int version;
        int capabilities;
        int charset;
        int low_version;
        int auth_failed;
    } server;
} db_mysql_session_t;

typedef struct db_mysql_connection_t {
    /*
     * Must be binary compatible with db_connection_t
     */
    struct db_mysql_connection_t* next;
    struct db_mysql_connection_t* prev;
    db_mysql_session_t* session;
    db_error_t error;
    struct db_mysql_result_t* result;

    /*
     * ToDo: add support to pipe, and shared memory
     */
    api_tcp_t tcp;
    uint64_t affected;
    uint64_t insert_id;
    /*
     * Connection is in undefined state. This will happen when data exchange
     * fails, or when it does not match protocol specification
     */
    int undefined;
} db_mysql_connection_t;

typedef struct db_mysql_statement_t {
    /*
     * Must be binary compatible with db_statement_t
     */
    db_mysql_connection_t* connection;

    /*
     * MySQL specific
     */
    int id;
    int num_params;
    db_column_t* params;
    db_value_t* values;
    int* mysql_types;
    int params_changed;
} db_mysql_statement_t;

typedef struct db_mysql_result_t {
    /*
     * Must be binary compatible with db_result_t
     */
    db_mysql_connection_t* connection;

    /*
     * MySQL specific
     */
    int num_columns;
    int num_rows;
    db_column_t* columns;
    db_value_t** rows;
    int* mysql_types;
    int has_more; // more resultset exists
    int by_fetch; // fetch rows by COM_STMT_FETCH
    int rows_done; // all rows was fetched in current resultset
    int statement_id;
} db_mysql_result_t;

/*
 * Macro
 */

#define PACKET_IS_OK(packet) (PACKET_OK == (unsigned char)*(packet).data)
#define PACKET_IS_EOF(packet) ((packet).size < 9 && PACKET_EOF == (unsigned char)*(packet).data)
#define PACKET_IS_ERROR(packet) (PACKET_ERROR == (unsigned char)*(packet).data)

/*
 * Helpers
 */

/*
 * Reads packet from connection.
 * Returns:
 *   DB_OK on success, call db_mysql_free after use
 *   DB_UNAVAILABLE on failure, and sets connection.undefined to 1
 */
int db_mysql_read(db_mysql_connection_t* connection, db_mysql_packet_t* packet);

/*
 * Sends packet to connection.
 * Returns:
 *   DB_OK on success, call db_mysql_free after use
 *   DB_UNAVAILABLE on failure, and sets connection.undefined to 1
 */
int db_mysql_write(db_mysql_connection_t* connection, db_mysql_packet_t* packet);

/*
 * Free packet payload
 */
void db_mysql_free(db_mysql_connection_t* connection, db_mysql_packet_t* packet);

/*
 * Read and return uint64_t encoded as lenenc
 * Params:
 *   buffer - source to read from
 *   count - out value, bytes count processed
 */
uint64_t db_mysql_read_lenencint(char* buffer, uint64_t* count);

/*
 * Write uint64_t inti buffer as lennecint
 * Returns pointer to buffer after data wrote
 */
char* db_mysql_write_lenencint(char* buffer, uint64_t value);

/*
 * Read lenencstr from buffer
 * Params:
 *   buffer - source to read from
 *   str - out value, null terminated string readed
 *   length - strlen(str)
 * Returns: number of bytes processed
 */
uint64_t db_mysql_read_lenencstr(api_pool_t* pool, char* buffer, char** str, uint64_t* length);

/*
 * Skip string encoded as lenencstr
 * Returns number of bytes processed
 */
uint64_t db_mysql_skip_lenencstr(char* buffer);

/*
 * Returns bytes needed to fit string with specified length
 */
uint64_t db_mysql_calc_lenencstr_size(uint64_t length);

/*
 * Parses packet as an error into parameter.
 */
void db_mysql_parse_error(api_pool_t* pool, db_mysql_packet_t* packet, db_error_t* error);

/*
 * Read status packet, e.g OK or ERROR ok EOF.
 * Returns:
 *   DB_OK on success, check status->code for packet type, call db_mysql_status_free after use
 *   DB_UNAVAILABLE or DB_UNKNOWN on failure, and sets connection.undefined to 1
 */
int db_mysql_status_read(db_mysql_connection_t* connection, db_mysql_status_t* status);

/*
 * Free memory used in status
 */
void db_mysql_status_free(api_pool_t* pool, db_mysql_status_t* status);

/*
 * map MYSQL_TYPE_* to DB_TYPE_*
 */
int db_mysql_detect_type(unsigned char mysql_type);

/*
 * Tryes to read first result packet after query, or exec.
 * If resultset available initializes connection.result field,
 * Else leaves it null
 */
int db_mysql_read_result(db_mysql_connection_t* connection, int statement_id);

/*
 * If there are resultsets available, fetch them all
 */
int db_mysql_eat_result(db_mysql_connection_t* connection);

/*
 * Interface declarations
 */

int db_mysql_session_start(api_loop_t* loop, db_engine_t* engine, db_mysql_session_t** session);
int db_mysql_session_error(db_mysql_session_t* session, db_error_t* error);
int db_mysql_session_close(db_mysql_session_t* session);

int db_mysql_connection_create(db_mysql_session_t* session, db_mysql_connection_t** connection);
int db_mysql_connection_open(db_mysql_session_t* session, db_mysql_connection_t** connection);
int db_mysql_connection_error(db_mysql_connection_t* connection, db_error_t* error);
int db_mysql_connection_query(db_mysql_connection_t* connection, const char* sql, db_mysql_result_t** result);
int db_mysql_connection_affected(db_mysql_connection_t* connection, uint64_t* affected);
int db_mysql_connection_insert_id(db_mysql_connection_t* connection, uint64_t* insert_id);
int db_mysql_connection_begin(db_mysql_connection_t* connection);
int db_mysql_connection_commit(db_mysql_connection_t* connection);
int db_mysql_connection_rollback(db_mysql_connection_t* connection);
int db_mysql_connection_close(db_mysql_connection_t* connection);
int db_mysql_connection_destroy(db_mysql_connection_t* connection);

int db_mysql_statement_prepare(db_mysql_connection_t* connection, const char* sql, db_mysql_statement_t** statement);
int db_mysql_statement_reset(db_mysql_statement_t* statement);
int db_mysql_statement_bind_null(db_mysql_statement_t* statement, int index);
int db_mysql_statement_bind_bool(db_mysql_statement_t* statement, int index, char value);
int db_mysql_statement_bind_byte(db_mysql_statement_t* statement, int index, char value);
int db_mysql_statement_bind_short(db_mysql_statement_t* statement, int index, short value);
int db_mysql_statement_bind_int(db_mysql_statement_t* statement, int index, int value);
int db_mysql_statement_bind_int64(db_mysql_statement_t* statement, int index, int64_t value);
int db_mysql_statement_bind_float(db_mysql_statement_t* statement, int index, float value);
int db_mysql_statement_bind_double(db_mysql_statement_t* statement, int index, double value);
int db_mysql_statement_bind_time(db_mysql_statement_t* statement, int index, db_time_t* value);
int db_mysql_statement_bind_date(db_mysql_statement_t* statement, int index, db_date_t* value);
int db_mysql_statement_bind_datetime(db_mysql_statement_t* statement, int index, db_date_t* value);
int db_mysql_statement_bind_timestamp(db_mysql_statement_t* statement, int index, db_date_t* value);
int db_mysql_statement_bind_string(db_mysql_statement_t* statement, int index, const char* value);
int db_mysql_statement_bind_binary(db_mysql_statement_t* statement, int index, void* value, uint64_t size);
int db_mysql_statement_bind_blob(db_mysql_statement_t* statement, int index, void* value, uint64_t size);
int db_mysql_statement_exec(db_mysql_statement_t* statement, db_mysql_result_t** result);
int db_mysql_statement_close(db_mysql_statement_t* statement);

int db_mysql_result_fetch_columns(db_mysql_result_t* result, db_column_t** columns, int* num_columns);
int db_mysql_result_fetch_rows(db_mysql_result_t* result, db_value_t*** rows, int* count);
int db_mysql_result_close(db_mysql_result_t* result);

#endif // DB_MYSQL_H_INCLUDED