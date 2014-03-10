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

#include <stdio.h> /* for sscanf */

#include "../api_list.h"
#include "db_mysql.h"

typedef struct db_mysql_row_node_t {
    struct db_mysql_row_node_t* next;
    struct db_mysql_row_node_t* prev;
    db_value_t* row;
} db_mysql_row_node_t;

/*
 * Reads value encoded as binary
 */
uint64_t db_mysql_read_value(api_pool_t* pool, char* buffer, db_column_t* column, db_value_t* value)
{
    unsigned char length;

    switch (column->type)
    {
    case DB_TYPE_BOOL:
    case DB_TYPE_BYTE:
        value->value_byte = *buffer;
        return 1;
    case DB_TYPE_SHORT:
        value->value_short = *(short*)buffer;
        return 2;
    case DB_TYPE_INT:
        value->value_int = *(int*)buffer;
        return 4;
    case DB_TYPE_INT64:
        value->value_int64 = *(int64_t*)buffer;
        return 8;
    case DB_TYPE_FLOAT:
        value->value_float = *(float*)buffer;
        return sizeof(float);
    case DB_TYPE_DOUBLE:
        value->value_double = *(double*)buffer;
        return sizeof(double);
    case DB_TYPE_TIME:
        length = *buffer;
        ++buffer;

        if (length >= 8)
        {
            value->value_time.is_negative = *buffer; buffer += 1;
            value->value_time.days = *(int*)buffer; buffer += 4;
            value->value_time.hours = *buffer; buffer += 1;
            value->value_time.minutes = *buffer; buffer += 1;
            value->value_time.seconds = *buffer; buffer += 1;
        }

        if (length >= 12)
        {
            value->value_time.microseconds = *(int*)buffer; buffer += 4;
        }

        return 1 + length;
    case DB_TYPE_DATE:
    case DB_TYPE_DATETIME:
    case DB_TYPE_TIMESTAMP:
        length = *buffer;
        ++buffer;

        if (length >= 4)
        {
            value->value_date.year = *(short*)buffer; buffer += 2;
            value->value_date.month = *buffer; buffer += 1;
            value->value_date.day = *buffer; buffer += 1;
        }

        if (length >= 7)
        {
            value->value_date.hour = *buffer; buffer += 1;
            value->value_date.minute = *buffer; buffer += 1;
            value->value_date.second = *buffer; buffer += 1;
        }

        if (length == 11)
        {
            value->value_date.microsecond = *(int*)buffer; buffer += 4;
        }

        return 1 + length;

    default: // DB_TYPE_STRING or DB_TYPE_BINARY
        return db_mysql_read_lenencstr(pool, buffer, &value->value_string, &value->size);
    }
}

uint64_t db_mysql_parse_numeric(char* buffer, int length)
{
    uint64_t value = 0;
    int i;

    for (i = 0; i < length; ++i)
    {
        value = 10 * value + (buffer[i] - '0');
    }

    return value;
}

/*
 * Reads value encoded as raw text
 */
uint64_t db_mysql_parse_value(api_pool_t* pool, char* buffer, db_column_t* column, db_value_t* value)
{
    uint64_t count;
    uint64_t length;
    char temp[100];  // large enough to store float, double values
    char* pos;

    length = db_mysql_read_lenencint(buffer, &count);

    switch (column->type)
    {
    case DB_TYPE_BOOL:
    case DB_TYPE_BYTE:
    case DB_TYPE_SHORT:
    case DB_TYPE_INT:
    case DB_TYPE_INT64:
        value->value_int64 = db_mysql_parse_numeric(buffer + count, length);
        break;
    case DB_TYPE_FLOAT:
        memcpy(temp, buffer + count, length);
        temp[length] = ' ';
        sscanf(temp, "%f", &value->value_float);
        break;
    case DB_TYPE_DOUBLE:
        memcpy(temp, buffer + count, length);
        temp[length] = ' ';
        sscanf(temp, "%lf", &value->value_double);
        break;
    case DB_TYPE_TIME:
        pos = buffer + count;
        value->value_time.hours = (pos[0] - '0') * 10 + (pos[1] - '0'); pos += 3;
        value->value_time.minutes = (pos[0] - '0') * 10 + (pos[1] - '0'); pos += 3;
        value->value_time.seconds = (pos[0] - '0') * 10 + (pos[1] - '0');
        break;
    case DB_TYPE_DATE:
    case DB_TYPE_DATETIME:
    case DB_TYPE_TIMESTAMP:
        pos = buffer + count;
        value->value_date.year = (pos[0] - '0') * 1000 + (pos[1] - '0') * 100 + (pos[2] - '0') * 10 + (pos[3] - '0'); pos += 5;
        value->value_date.month = (pos[0] - '0') * 10 + (pos[1] - '0'); pos += 3;
        value->value_date.day = (pos[0] - '0') * 10 + (pos[1] - '0'); pos += 3;
        value->value_date.hour = (pos[0] - '0') * 10 + (pos[1] - '0'); pos += 3;
        value->value_date.minute = (pos[0] - '0') * 10 + (pos[1] - '0'); pos += 3;
        value->value_date.second = (pos[0] - '0') * 10 + (pos[1] - '0');
        break;
    default: // DB_TYPE_STRING or DB_TYPE_BINARY
        db_mysql_read_lenencstr(pool, buffer, &value->value_string, &value->size);
        break;
    }

    return length + count;
}

db_value_t* db_mysql_result_read_row(db_mysql_result_t* result, db_mysql_packet_t* packet)
{
    api_pool_t* pool = api_pool_default(result->connection->session->base.loop);
    db_value_t* row = (db_value_t*)api_calloc(pool, result->num_columns * sizeof(db_value_t));
    char* pos;
    int i;

    if (result->statement_id > 0)
    {
        /*
         * Read binary protocol rows
         */

        /*
         * Skip '\0' (OK code)
         */
        pos = packet->data + 1;

        /*
         * null bitmap
         */
        for (i = 0; i < result->num_columns; ++i)
        {
            row[i].is_null = 0 != (pos[i / 8] & (i % 8));
        }

        /*
         * values. null bitmap length: (column-count + 7 + 2) / 8
         */
        pos += (result->num_columns + 7 + 2) / 8;

        for (i = 0; i < result->num_columns; ++i)
        {
            if (!row[i].is_null)
            {
                pos += db_mysql_read_value(pool, pos, result->columns + i, row + i);
            }
        }
    }
    else
    {
        /*
         * Read text protocol rows
         */

        pos = packet->data;

        for (i = 0; i < result->num_columns; ++i)
        {
            if ((unsigned char)*pos == 0xfb)
            {
                // null
                row[i].is_null = 1;
                ++pos;
            }
            else
            {
                row[i].is_null = 0;
                pos += db_mysql_parse_value(pool, pos, result->columns + i, row + i);
            }
        }
    }

    return row;
}

void db_mysql_result_free_columns(db_mysql_result_t* result)
{
    api_pool_t* pool = api_pool_default(result->connection->session->base.loop);
    int i;

    if (result->num_columns > 0 && result->columns != 0)
    {
        for (i = 0; i < result->num_columns; ++i)
            if (result->columns[i].name)
            {
                api_free(pool, strlen(result->columns[i].name) + 1, result->columns[i].name);
                result->columns[i].name = 0;
            }

        api_free(pool, result->num_columns * sizeof(db_column_t), result->columns);
        api_free(pool, result->num_columns * sizeof(int), result->mysql_types);

        result->columns = 0;
        result->num_columns = 0;
    }
}

void db_mysql_result_free_rows(db_mysql_result_t* result)
{
    api_pool_t* pool = api_pool_default(result->connection->session->base.loop);
    int i, j;

    if (result->num_rows > 0)
    {
        for (i = 0; i < result->num_rows; ++i)
        {
            for (j = 0; j < result->num_columns; ++j)
            {
                if (!result->rows[i][j].is_null && (result->columns[i].type == DB_TYPE_STRING || result->columns[i].type == DB_TYPE_BINARY))
                {
                    api_free(pool, result->rows[i][j].size + 1, result->rows[i][j].value_string);
                }
            }

            api_free(pool, result->num_columns * sizeof(db_value_t), result->rows[i]);
        }

        api_free(pool, result->num_rows * sizeof(db_value_t*), result->rows);

        result->num_rows = 0;
    }
}

int db_mysql_eat_result(db_mysql_connection_t* connection)
{
    api_pool_t* pool = api_pool_default(connection->session->base.loop);
    db_column_t* columns;
    db_value_t** rows;
    int num_columns;
    int num_rows;
    int count;
    int code = DB_OK;

    if (connection->result == 0)
        return DB_OK;

    if (connection->undefined)
    {
        db_mysql_result_free_columns(connection->result);
        db_mysql_result_free_rows(connection->result);
        api_free(pool, sizeof(*connection->result), connection->result);
        connection->result = 0;

        return DB_UNAVAILABLE;
    }

    if (connection->result->num_columns > 0 && connection->result->columns == 0)
    {
        /*
         * Fetched first resultsets column count
         */
    }
    else if (!connection->result->rows_done)
    {
        /*
         * Called in the middle of rows
         */

        while (DB_OK == code && !connection->result->rows_done)
        {
            code = db_mysql_result_fetch_rows(connection->result, &rows, &count);
        }
    }

    /*
     * eat next resultsets
     */
    while (db_result_fetch_columns((db_result_t*)connection->result, &columns, &num_columns) == DB_OK)
    {
        num_rows = 0;
        while (db_result_fetch_rows((db_result_t*)connection->result, &rows, &num_rows) == DB_OK && num_rows > 0)
        {
        }
    }

    db_mysql_result_free_columns(connection->result);
    db_mysql_result_free_rows(connection->result);
    api_free(pool, sizeof(*connection->result), connection->result);
    connection->result = 0;

    if (connection->undefined)
        code = DB_UNAVAILABLE;

    return code;
}

int db_mysql_result_fetch_columns(db_mysql_result_t* result, db_column_t** columns, int* num_columns)
{
    api_pool_t* pool = api_pool_default(result->connection->session->base.loop);
    db_mysql_packet_t packet;
    db_mysql_status_t status;
    uint64_t pos;
    int code = DB_OK;
    int i;

    *columns = 0;
    *num_columns = 0;

    /*
     * Clear columns and rows of last resultset
     */
    db_mysql_result_free_columns(result);
    db_mysql_result_free_rows(result);

    if (result->connection->undefined)
    {
        /*
         * Connection in invalid state
         */
        return DB_UNKNOWN;
    }

    if (result->num_columns > 0 && result->columns == 0)
    {
        /*
         * Fetched first resultsets column count
         */
    }
    else if (!result->rows_done)
    {
        /*
         * Called in the middle of rows
         */
        return DB_OUT_OF_SYNC;
    }
    else if (!result->has_more)
    {
        /*
         * No more resultset
         */
        return DB_NO_DATA;
    }
    else
    {
        /*
         * Reading next resultset, fetch columns count first
         */
        code = db_mysql_read(result->connection, &packet);
        if (DB_OK != code)
            return code;

        if (PACKET_IS_ERROR(packet))
        {
            /*
                * On failure, set error status in connection and exit
                */
            db_mysql_parse_error(pool, &packet, &result->connection->error);
            db_mysql_free(result->connection, &packet);
            return DB_FAILED;
        }

        /*
         * Read Columns Count
         */
        result->num_columns = (int)db_mysql_read_lenencint(packet.data, &pos);
        db_mysql_free(result->connection, &packet);
    }

    /*
     * Reset per resultset flags
     */
    result->by_fetch = 0;
    result->rows_done = 0;
    result->has_more = 0;

    if (result->num_columns == 0)
    {
        /*
         * This can happen on next resultset
         */
        return DB_NO_DATA;
    }

    /*
     * Fetch columns
     */

    result->columns = (db_column_t*)api_calloc(pool, result->num_columns * sizeof(*result->columns));
    result->mysql_types = (int*)api_calloc(pool, result->num_columns * sizeof(int));

    i = 0;
    while (i < result->num_columns)
    {
        if (DB_OK == db_mysql_read(result->connection, &packet))
        {
            pos = db_mysql_skip_lenencstr(packet.data);        // catalog
            pos += db_mysql_skip_lenencstr(packet.data + pos); // schema
            pos += db_mysql_skip_lenencstr(packet.data + pos); // table
            pos += db_mysql_skip_lenencstr(packet.data + pos); // org_table
            pos += db_mysql_read_lenencstr(pool, packet.data + pos, &result->columns[i].name, 0);
            pos += db_mysql_skip_lenencstr(packet.data + pos); // org_name
            pos += 1; // 0x0c
            pos += 2; // charset

            result->columns[i].length = *(uint32_t*)(packet.data + pos);
            pos += 4;
            result->mysql_types[i] = *(packet.data + pos);
            result->columns[i].type = db_mysql_detect_type(result->mysql_types[i]);

            db_mysql_free(result->connection, &packet);

            ++i;
        }
        else
        {
            code = DB_UNAVAILABLE;
            break;
        }
    }

    if (i != result->num_columns)
    {
        /*
         * In case of partial read, cleanup names, and types
         */
        while (--i >= 0)
        {
            if (result->columns[i].name)
                api_free(pool, strlen(result->columns[i].name) + 1, result->columns[i].name);

            result->columns[i].name = 0;
        }

        api_free(pool, result->num_columns * sizeof(*result->columns), result->columns);
        api_free(pool, result->num_columns * sizeof(int), result->mysql_types);

        result->num_columns = 0;
        result->num_rows = 0;
        result->connection->undefined = 1;
        code = DB_UNAVAILABLE;
    }

    /*
     * Columns end up with EOF
     */
    if (DB_OK == code)
    {
        code = db_mysql_status_read(result->connection, &status);

        if (DB_OK == code)
        {
            if (status.code == PACKET_EOF)
            {
                if (SERVER_STATUS_CURSOR_EXISTS == (status.eof.flags & SERVER_STATUS_CURSOR_EXISTS))
                    result->by_fetch = 1;
            }
            else
            {
                /*
                 * Not OK, not EOF, this is not documented
                 */
                db_mysql_result_free_columns(result);
                result->connection->undefined = 1;
                code = DB_UNKNOWN;
            }
        }

        db_mysql_status_free(pool, &status);
    }

    db_mysql_free(result->connection, &packet);

    if (DB_OK == code)
    {
        *num_columns = result->num_columns;
        *columns = result->columns;
    }
    else
    {
        db_mysql_result_free_columns(result);
    }

    return code;
}

int db_mysql_result_fetch_rows(db_mysql_result_t* result, db_value_t*** rows, int* count)
{
    api_pool_t* pool = api_pool_default(result->connection->session->base.loop);
    db_mysql_packet_t packet;
    api_list_t list;
    db_mysql_row_node_t* node;
    char cmd_fetch[4 + 9];
    int code = DB_OK;
    int nrow = 0;
    int i;

    *rows = 0;

    list.head = 0;
    list.tail = 0;

    if (result->connection->undefined)
    {
        /*
         * Connection in invalid state
         */
        return DB_UNKNOWN;
    }

    if (result->columns == 0)
    {
        /*
         * Firt db_result_fetch_columns must be called
         */
        return DB_OUT_OF_SYNC;
    }

    if (result->rows_done)
    {
        return DB_NO_DATA;
    }

    /*
     * Free previous rows
     */
    db_mysql_result_free_rows(result);

    /*
     * If Fetch needed, then send CMD_STM_FETCH command
     */
    if (result->by_fetch)
    {
        *(int*)cmd_fetch = 9;
        cmd_fetch[4] = COM_STMT_FETCH;
        *(int*)(cmd_fetch + 5) = result->statement_id;
        *(int*)(cmd_fetch + 9) = *count;

        if (4 + 9 != api_stream_write(&result->connection->tcp.stream, cmd_fetch, 4 + 9))
        {
            db_mysql_result_free_columns(result);
            db_mysql_result_free_rows(result);
            result->connection->undefined = 1;
            return DB_UNAVAILABLE;
        }
    }

    nrow = 0;
    while (*count == 0 || nrow < *count)
    {
        code = db_mysql_read(result->connection, &packet);
        if (DB_OK != code)
        {
            break;
        }

        if (PACKET_IS_ERROR(packet))
        {
            db_mysql_parse_error(pool, &packet, &result->connection->error);
            db_mysql_free(result->connection, &packet);
            db_mysql_result_free_columns(result);
            db_mysql_result_free_rows(result);
            result->connection->undefined = 1;
            code = DB_FAILED;
            break;
        }

        if (PACKET_IS_EOF(packet))
        {
            //if (SERVER_STATUS_LAST_ROW_SENT == (SERVER_STATUS_LAST_ROW_SENT & *(short*)(packet.data + 3)))
            //    result->rows_done = 1;

            if (SERVER_MORE_RESULTS_EXISTS == (SERVER_MORE_RESULTS_EXISTS & *(short*)(packet.data + 3)))
                result->has_more = 1;
            
            db_mysql_free(result->connection, &packet);
            result->rows_done = 1;
            code = DB_OK;
            break;
        }

        node = (db_mysql_row_node_t*)api_alloc(pool, sizeof(*node));
        node->row = db_mysql_result_read_row(result, &packet);
        api_list_push_tail(&list, (api_node_t*)node);

        ++nrow;
        db_mysql_free(result->connection, &packet);
    }

    if (nrow > 0)
    {
        result->rows = (db_value_t**)api_calloc(pool, nrow * sizeof(db_value_t*));
        for (i = 0; i < nrow; ++i)
        {
            node = (db_mysql_row_node_t*)api_list_pop_head(&list);
            result->rows[i] = node->row;

            api_free(pool, sizeof(*node), node);
        }
    }

    *rows = result->rows;
    *count = nrow;

    return code;
}

int db_mysql_result_close(db_mysql_result_t* result)
{
    return db_mysql_eat_result(result->connection);
}