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

void db_mysql_statement_free_values(api_pool_t* pool, db_mysql_statement_t* statement)
{
    int i;

    for (i = 0; i < statement->num_params; ++i)
    {
        if (0 != statement->values[i].is_null)
        {
            statement->params_changed = 1;

            switch (statement->params[i].type)
            {
            case DB_TYPE_STRING:
            case DB_TYPE_BINARY:
                /*
                 * For binary & string we reserve one more char for trailing '\0'
                 */
                if (statement->values[i].size > 0)
                    api_free(pool, statement->values[i].size + 1, statement->values[i].value_string);
                statement->values[i].size = 0;
                break;
            }

            statement->values[i].is_null = 1;
        }
    }
}

void db_mysql_statement_free(api_pool_t* pool, db_mysql_statement_t* statement)
{
    int i;

    db_mysql_statement_free_values(pool, statement);

    for (i = 0; i < statement->num_params; ++i)
    {
        if (statement->params[i].name)
            api_free(pool, strlen(statement->params[i].name) + 1, statement->params[i].name);
    }

    if (statement->num_params > 0)
    {
        api_free(pool, statement->num_params * sizeof(db_column_t), statement->params);
        api_free(pool, statement->num_params * sizeof(db_value_t), statement->values);
        api_free(pool, statement->num_params * sizeof(int), statement->mysql_types);
    }

    api_free(pool, sizeof(*statement), statement);
}

int db_mysql_statement_bind_natural(db_mysql_statement_t* statement, int index, int64_t value)
{
    if (index < 0 || index >= statement->num_params)
        return DB_OUT_OF_INDEX;

    switch (statement->params[index].type)
    {
    case DB_TYPE_BOOL:
    case DB_TYPE_BYTE:
    case DB_TYPE_SHORT:
    case DB_TYPE_INT:
    case DB_TYPE_INT64:
        if (statement->values[index].is_null || (statement->values[index].value_int64 != value))
        {
            statement->values[index].is_null = 0;
            statement->values[index].value_int64 = value;
            statement->params_changed = 1;
        }

        break;

    default:
        /*
         * param type not compatible with value type
         */
        return DB_MISMATCH;
    }

    return DB_OK;
}

/*
 * Interface implementations
 */

int db_mysql_statement_prepare(db_mysql_connection_t* connection, const char* sql, db_mysql_statement_t** statement)
{
    api_pool_t* pool = api_pool_default(connection->session->base.loop);
    int sql_length = strlen(sql);
    char command[5];
    db_mysql_packet_t packet;
    db_mysql_status_t status;
    int error = 0;
    int i;
    uint64_t pos = 0;
    int num_columns = 0;

    *statement = 0;

    if (connection->undefined)
        return DB_UNKNOWN;

    /*
     * Eat pending resultsets
     */
    db_mysql_eat_result(connection);

    /*
     * Send COM_PREPARE command
     */

    *(int*)command = 1 /* COM_STMT_PREPARE */
                   + sql_length;

    command[3] = 0; // reset sequence
    command[4] = COM_STMT_PREPARE;

    /* header and code */
    if (5 != api_stream_write(&connection->tcp.stream, command, 5))
    {
        connection->undefined = 1;
        return DB_UNAVAILABLE;
    }

    /* sql */
    if (sql_length != api_stream_write(&connection->tcp.stream, sql, sql_length))
    {
        connection->undefined = 1;
        return DB_UNAVAILABLE;
    }

    /*
     * Parse statement
     */

    error = db_mysql_read(connection, &packet);
    if (DB_OK != error)
        return error;

    if ( ! PACKET_IS_OK(packet))
    {
        error = DB_UNKNOWN;

        if (PACKET_IS_ERROR(packet))
        {
            /*
             * Parse error
             */
            db_mysql_parse_error(pool, &packet, &connection->error);
            error = DB_FAILED;
        }

        db_mysql_free(connection, &packet);
        return error;
    }

    *statement = (db_mysql_statement_t*)api_calloc(pool, sizeof(**statement));
    (*statement)->connection = connection;
    (*statement)->id = *(int*)(packet.data + 1);
    num_columns = *(short*)(packet.data + 1 + 4);
    (*statement)->num_params = *(short*)(packet.data + 1 + 4 + 2);

    db_mysql_free(connection, &packet);

    /* 
     * Parse params & columns
     * http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition
     */
    
    if ((*statement)->num_params > 0)
    {
        (*statement)->params = (db_column_t*)api_calloc(pool, (*statement)->num_params * sizeof(db_column_t));
        (*statement)->values = (db_value_t*)api_calloc(pool, (*statement)->num_params * sizeof(db_value_t));
        (*statement)->mysql_types = (int*)api_calloc(pool, (*statement)->num_params * sizeof(int));

        i = 0;
        while (i < (*statement)->num_params)
        {
            if (DB_OK == db_mysql_read(connection, &packet))
            {
                pos = db_mysql_skip_lenencstr(packet.data);        // catalog
                pos += db_mysql_skip_lenencstr(packet.data + pos); // schema
                pos += db_mysql_skip_lenencstr(packet.data + pos); // table
                pos += db_mysql_skip_lenencstr(packet.data + pos); // org_table
                pos += db_mysql_read_lenencstr(pool, packet.data + pos, &(*statement)->params[i].name, 0);
                pos += db_mysql_skip_lenencstr(packet.data + pos); // org_name
                pos += 1; // 0x0c
                pos += 2; // charset

                (*statement)->params[i].length = *(uint32_t*)(packet.data + pos);
                pos += 4;
                (*statement)->mysql_types[i] = *(packet.data + pos);

                (*statement)->params[i].type = db_mysql_detect_type((*statement)->mysql_types[i]);
                (*statement)->values[i].is_null = 1;

                db_mysql_free(connection, &packet);

                ++i;
            }
            else
            {
                error = DB_UNAVAILABLE;
                break;
            }
        }

        if (i != (*statement)->num_params)
        {
            /*
             * In case of partial read, cleanup names, and types
             */
            while (--i >= 0)
            {
                if ((*statement)->params[i].name)
                    api_free(pool, strlen((*statement)->params[i].name) + 1, (*statement)->params[i].name);

                (*statement)->params[i].name = 0;
            }

            /*
             * For correct handling in db_mysql_statement_free
             */
            for (i = 0; i < (*statement)->num_params; ++i)
            {
                (*statement)->params[i].type = DB_TYPE_INT;
                (*statement)->values[i].is_null = 1;
                (*statement)->mysql_types[i] = MYSQL_TYPE_NULL;
            }

            connection->undefined = 1;
            error = DB_UNAVAILABLE;
        }

        /*
         * Params end up with EOF
         */
        if (DB_OK == error)
        {
            error = db_mysql_status_read(connection, &status);

            if (DB_OK == error)
            {
                if (status.code != PACKET_EOF)
                {
                    /*
                     * Not OK, not EOF, this is not documented
                     */
                    connection->undefined = 1;
                    error = DB_UNKNOWN;
                }
            }

            db_mysql_status_free(pool, &status);
        }
    }


    if (DB_OK == error && num_columns > 0)
    {
        /*
         * Skip columns with checking
         */

        i = 0;
        while (i < num_columns)
        {
            if (DB_OK == db_mysql_read(connection, &packet))
            {
                db_mysql_free(connection, &packet);

                ++i;
            }
            else
            {
                error = DB_UNAVAILABLE;
                break;
            }
        }

        if (i != num_columns)
        {
            connection->undefined = 1;
            error = DB_UNAVAILABLE;
        }

        /*
         * Columns end up with EOF
         */
        if (DB_OK == error)
        {
            error = db_mysql_status_read(connection, &status);

            if (DB_OK == error)
            {
                if (status.code != PACKET_EOF)
                {
                    /*
                     * Not OK, not EOF, this is not documented
                     */
                    connection->undefined = 1;
                    error = DB_UNKNOWN;
                }
            }

            db_mysql_status_free(pool, &status);
        }
    }

    if (DB_OK != error)
    {
        db_mysql_statement_free(pool, *statement);

        *statement = 0;
    }

    return error;
}

int db_mysql_statement_reset(db_mysql_statement_t* statement)
{
    api_pool_t* pool = api_pool_default(statement->connection->session->base.loop);
    db_mysql_status_t status;
    char command[9];
    int i;

    /*
     * Eat pending resultsets
     */
    db_mysql_eat_result(statement->connection);

    db_mysql_statement_free_values(pool, statement);

    command[0] = 5; // payload size
    command[1] = 0;
    command[2] = 0;
    command[3] = 0; // sequence number
    command[4] = COM_STMT_RESET;
    *(int*)(command + 5) = statement->id;

    if (9 != api_stream_write(&statement->connection->tcp.stream, command, 9))
    {
        statement->connection->undefined = 1;
        return DB_UNAVAILABLE;
    }

    if (DB_OK != db_mysql_status_read(statement->connection, &status))
    {
        statement->connection->undefined = 1;
        return DB_UNAVAILABLE;
    }

    if (status.code == PACKET_ERROR)
    {
        db_error_override(pool, &statement->connection->error, &status.err);
    }

    db_mysql_status_free(pool, &status);
    
    if (status.code == PACKET_ERROR)
        return DB_FAILED;
    else if (status.code == PACKET_OK)
    {
        for (i = 0; i < statement->num_params; ++i)
        {
            /*
             * Call through iface, in case when iface was hooked
             */
            statement->connection->session->base.iface.statement.bind_null((db_statement_t*)statement, 0);
        }

        return DB_OK;
    }

    statement->connection->undefined = 1;
    return DB_UNKNOWN;
}

int db_mysql_statement_bind_null(db_mysql_statement_t* statement, int index)
{
    api_pool_t* pool = api_pool_default(statement->connection->session->base.loop);

    if (index < 0 || index >= statement->num_params)
        return DB_OUT_OF_INDEX;

    if (statement->values[index].is_null)
    {
        /*
         * Already null
         */
        return DB_OK;
    }

    statement->values[index].is_null = 1;
    statement->params_changed = 1;

    switch (statement->params[index].type) {
    case DB_TYPE_STRING:
    case DB_TYPE_BINARY:
        /*
         * Free memory for pointer types
         */
        api_free(pool, statement->values[index].size + 1, statement->values[index].value_binary);
        statement->values[index].value_binary = 0;
        statement->values[index].size = 0;
        return DB_OK;
    }

    /*
     * For value types, we already set is_null = 1
     */

    return DB_OK;
}

int db_mysql_statement_bind_bool(db_mysql_statement_t* statement, int index, char value)
{
    if (index < 0 || index >= statement->num_params)
        return DB_OUT_OF_INDEX;

    if (0 != (value & 0xfe))
        return DB_TOO_LONG;

    return db_mysql_statement_bind_natural(statement, index, value);
}

int db_mysql_statement_bind_byte(db_mysql_statement_t* statement, int index, char value)
{
    if (index < 0 || index >= statement->num_params)
        return DB_OUT_OF_INDEX;

    switch (statement->params[index].type)
    {
    case DB_TYPE_BOOL:
        if (0 != (value & 0xfe))
            return DB_TOO_LONG;
        break;
    }

    return db_mysql_statement_bind_natural(statement, index, value);
}

int db_mysql_statement_bind_short(db_mysql_statement_t* statement, int index, short value)
{
    if (index < 0 || index >= statement->num_params)
        return DB_OUT_OF_INDEX;

    switch (statement->params[index].type)
    {
    case DB_TYPE_BOOL: 
        if (0 != (value & 0xfffe))
            return DB_TOO_LONG;
        break;

    case DB_TYPE_BYTE:
        if (0 != (value & 0xff00))
            return DB_TOO_LONG;
        break;
    }

    return db_mysql_statement_bind_natural(statement, index, value);
}

int db_mysql_statement_bind_int(db_mysql_statement_t* statement, int index, int value)
{
    if (index < 0 || index >= statement->num_params)
        return DB_OUT_OF_INDEX;

    switch (statement->params[index].type)
    {
    case DB_TYPE_BOOL:
        if (0 != (value & 0xfffffffe))
            return DB_TOO_LONG;
        break;

    case DB_TYPE_BYTE:
        if (0 != (value & 0xffffff00))
            return DB_TOO_LONG;
        break;

    case DB_TYPE_SHORT:
        if (0 != (value & 0xffff0000))
            return DB_TOO_LONG;
        break;

    }

    return db_mysql_statement_bind_natural(statement, index, value);
}

int db_mysql_statement_bind_int64(db_mysql_statement_t* statement, int index, int64_t value)
{
    if (index < 0 || index >= statement->num_params)
        return DB_OUT_OF_INDEX;

    switch (statement->params[index].type)
    {
    case DB_TYPE_BOOL:
        if (0 != (value & 0xfffffffffffffffe))
            return DB_TOO_LONG;
        break;

    case DB_TYPE_BYTE:
        if (0 != (value & 0xffffffffffffff00))
            return DB_TOO_LONG;
        break;

    case DB_TYPE_SHORT:
        if (0 != (value & 0xffffffffffff0000))
            return DB_TOO_LONG;
        break;

    case DB_TYPE_INT:
        if (0 != (value & 0xffffffff00000000))
            return DB_TOO_LONG;
        break;
    }

    return db_mysql_statement_bind_natural(statement, index, value);
}

int db_mysql_statement_bind_float(db_mysql_statement_t* statement, int index, float value)
{
    int is_equal = 0;
    int is_null = 0;

    if (index < 0 || index >= statement->num_params)
        return DB_OUT_OF_INDEX;

    is_null = statement->values[index].is_null;

    switch (statement->params[index].type)
    {
    case DB_TYPE_FLOAT:
        is_equal = !is_null && (statement->values[index].value_float == value);

        if (!is_equal)
            statement->values[index].value_float = value;

        break;

    case DB_TYPE_DOUBLE:
        is_equal = !is_null && (statement->values[index].value_double == value);

        if (!is_equal)
            statement->values[index].value_double = value;

        break;

    default:
        /*
         * param type not compatible with value type
         */
        return DB_MISMATCH;
    }

    if (!is_equal)
    {
        statement->values[index].is_null = 0;
        statement->params_changed = 1;
    }

    return DB_OK;
}

int db_mysql_statement_bind_double(db_mysql_statement_t* statement, int index, double value)
{
    int is_equal = 0;
    int is_null = 0;

    if (index < 0 || index >= statement->num_params)
        return DB_OUT_OF_INDEX;

    is_null = statement->values[index].is_null;

    switch (statement->params[index].type)
    {
    case DB_TYPE_FLOAT:
        is_equal = !is_null && (statement->values[index].value_float == value);

        if (!is_equal)
            statement->values[index].value_float = value;

        break;

    case DB_TYPE_DOUBLE:
        is_equal = !is_null && (statement->values[index].value_double == value);

        if (!is_equal)
            statement->values[index].value_double = value;

        break;

    default:
        /*
         * param type not compatible with value type
         */
        return DB_MISMATCH;
    }

    if (!is_equal)
    {
        statement->values[index].is_null = 0;
        statement->params_changed = 1;
    }

    return DB_OK;
}

int db_mysql_statement_bind_time(db_mysql_statement_t* statement, int index, db_time_t* value)
{
    int is_equal = 0;
    int is_null = 0;

    if (index < 0 || index >= statement->num_params)
        return DB_OUT_OF_INDEX;

    is_null = statement->values[index].is_null;

    switch (statement->params[index].type)
    {
    case DB_TYPE_TIME:
        is_equal = !is_null && 0 == memcmp(&statement->values[index].value_time, value, sizeof(*value));

        if (!is_equal)
            memcpy(&statement->values[index].value_time, value, sizeof(*value));

        break;

    default:
        /*
         * param type not compatible with value type
         */
        return DB_MISMATCH;
    }

    if (!is_equal)
    {
        statement->values[index].is_null = 0;
        statement->params_changed = 1;
    }

    return DB_OK;
}

int db_mysql_statement_bind_date(db_mysql_statement_t* statement, int index, db_date_t* value)
{
    int is_equal = 0;
    int is_null = 0;

    if (index < 0 || index >= statement->num_params)
        return DB_OUT_OF_INDEX;

    is_null = statement->values[index].is_null;

    switch (statement->params[index].type)
    {
    case DB_TYPE_DATE:
        is_equal = !is_null && 0 == memcmp(&statement->values[index].value_date, value, sizeof(*value));

        if (!is_equal)
            memcpy(&statement->values[index].value_date, value, sizeof(*value));

        break;

    default:
        /*
         * param type not compatible with value type
         */
        return DB_MISMATCH;
    }

    if (!is_equal)
    {
        statement->values[index].is_null = 0;
        statement->params_changed = 1;
    }

    return DB_OK;
}

int db_mysql_statement_bind_datetime(db_mysql_statement_t* statement, int index, db_date_t* value)
{
    int is_equal = 0;
    int is_null = 0;

    if (index < 0 || index >= statement->num_params)
        return DB_OUT_OF_INDEX;

    is_null = statement->values[index].is_null;

    switch (statement->params[index].type)
    {
    case DB_TYPE_DATETIME:
        is_equal = !is_null && 0 == memcmp(&statement->values[index].value_datetime, value, sizeof(*value));

        if (!is_equal)
            memcpy(&statement->values[index].value_datetime, value, sizeof(*value));

        break;

    default:
        /*
         * param type not compatible with value type
         */
        return DB_MISMATCH;
    }

    if (!is_equal)
    {
        statement->values[index].is_null = 0;
        statement->params_changed = 1;
    }

    return DB_OK;
}

int db_mysql_statement_bind_timestamp(db_mysql_statement_t* statement, int index, db_date_t* value)
{
    int is_equal = 0;
    int is_null = 0;

    if (index < 0 || index >= statement->num_params)
        return DB_OUT_OF_INDEX;

    is_null = statement->values[index].is_null;

    switch (statement->params[index].type)
    {
    case DB_TYPE_TIMESTAMP:
        is_equal = !is_null && 0 == memcmp(&statement->values[index].value_timestamp, value, sizeof(*value));

        if (!is_equal)
            memcpy(&statement->values[index].value_timestamp, value, sizeof(*value));

        break;

    default:
        /*
         * param type not compatible with value type
         */
        return DB_MISMATCH;
    }

    if (!is_equal)
    {
        statement->values[index].is_null = 0;
        statement->params_changed = 1;
    }

    return DB_OK;
}

int db_mysql_statement_bind_string(db_mysql_statement_t* statement, int index, const char* value)
{
    return db_mysql_statement_bind_binary(statement, index, (void*)value, strlen(value));
}

int db_mysql_statement_bind_binary(db_mysql_statement_t* statement, int index, void* value, uint64_t size)
{
    int is_equal = 0;
    int is_null = 0;
    api_pool_t* pool = api_pool_default(statement->connection->session->base.loop);

    if (index < 0 || index >= statement->num_params)
        return DB_OUT_OF_INDEX;

    is_null = statement->values[index].is_null;

    switch (statement->params[index].type)
    {
    case DB_TYPE_STRING:
    case DB_TYPE_BINARY:
        is_equal = (is_null && size > 0 || !is_null && size == 0) &&
                size == statement->values[index].size &&
                0 == memcmp(statement->values[index].value_string, value, size);

        if (!is_equal)
        {
            if (statement->values[index].size > 0)
                api_free(pool, statement->values[index].size + 1, statement->values[index].value_string);

            statement->values[index].size = 0;
            statement->values[index].value_string = 0;
            statement->values[index].is_null = size == 0;

            if (size > 0)
            {
                statement->values[index].size = size;
                statement->values[index].value_string = (char*)api_alloc(pool, size + 1);
                memcpy(statement->values[index].value_string, value, size);
                statement->values[index].value_string[statement->values[index].size] = 0;
            }
        }

        break;

    default:
        /*
         * param type not compatible with value type
         */
        return DB_MISMATCH;
    }

    if (!is_equal)
    {
        statement->params_changed = 1;
    }

    return DB_OK;
}

int db_mysql_statement_bind_blob(db_mysql_statement_t* statement, int index, void* value, uint64_t size)
{
    api_pool_t* pool = api_pool_default(statement->connection->session->base.loop);
    char command[4 + 7];

    if (index < 0 || index >= statement->num_params)
        return DB_OUT_OF_INDEX;

    if (statement->connection->undefined)
        return DB_UNKNOWN;

    command[0] = 7; // payload size
    command[1] = 0;
    command[2] = 0;
    command[3] = 0; // sequence number
    command[4] = COM_STMT_LONG_DATA;
    *(int*)(command + 5) = statement->id;
    *(short*)(command + 5 + 4) = index;

    if (4 + 7 != api_stream_write(&statement->connection->tcp.stream, command, 4 + 7))
    {
        statement->connection->undefined = 1;
        return DB_UNAVAILABLE;
    }

    if (size != api_stream_write(&statement->connection->tcp.stream, (char*)value, size))
    {
        statement->connection->undefined = 1;
        return DB_UNAVAILABLE;
    }

    return DB_OK;
}

int db_mysql_statement_exec(db_mysql_statement_t* statement, db_mysql_result_t** result)
{
    api_pool_t* pool = api_pool_default(statement->connection->session->base.loop);
    db_mysql_packet_t packet;
    char* pos = 0;
    int i;
    int code;

    if (result)
        *result = 0;

    /*
     * Eat pending resultsets
     */
    db_mysql_eat_result(statement->connection);

    packet.sequence = 0;

    /*
     * Calculate payload size in bytes
     */

    packet.size = 1  // COM_STMT_EXECUTE
                + 4  // statement id
                + 1  // flags
                + 4; // iteration count = 0

    if (statement->num_params > 0)
    {
        packet.size += (statement->num_params + 7) / 8; // null mask
        packet.size += 1; // new params bound

        if (statement->params_changed)
        {
            packet.size += statement->num_params * 2; // types

            for (i = 0; i < statement->num_params; ++i)
            {
                if (!statement->values[i].is_null)
                {
                    switch (statement->params[i].type) {
                    case DB_TYPE_BOOL:
                    case DB_TYPE_BYTE:
                        packet.size += 1;
                        break;
                    case DB_TYPE_SHORT:
                        packet.size += 2;
                        break;
                    case DB_TYPE_INT:
                    case DB_TYPE_FLOAT:
                        packet.size += 4; 
                        break;
                    case DB_TYPE_INT64:
                    case DB_TYPE_DOUBLE:
                        packet.size += 8; 
                        break;
                    case DB_TYPE_TIME:
                        if (statement->values[i].value_time.days == 0 &&
                            statement->values[i].value_time.hours == 0 &&
                            statement->values[i].value_time.minutes == 0 &&
                            statement->values[i].value_time.seconds == 0 &&
                            statement->values[i].value_time.microseconds == 0)
                        {
                            packet.size += 1; // [0]
                        }
                        else
                        {
                            if (statement->values[i].value_time.microseconds == 0)
                            {
                                packet.size += 9; // [8][+/-][dddd][h][m][s]
                            }
                            else
                            {
                                packet.size += 13; // [12][+/-][dddd][h][m][s][uuuu]
                            }
                        }
                        break;
                    case DB_TYPE_DATE:
                    case DB_TYPE_DATETIME:
                    case DB_TYPE_TIMESTAMP: 
                        if (statement->values[i].value_date.year == 0 &&
                            statement->values[i].value_date.month == 0 &&
                            statement->values[i].value_date.day == 0 &&
                            statement->values[i].value_date.hour == 0 &&
                            statement->values[i].value_date.minute == 0 &&
                            statement->values[i].value_date.second == 0 &&
                            statement->values[i].value_date.microsecond == 0)
                        {
                            packet.size += 1; // [0]
                        }
                        else
                        {
                            if (statement->values[i].value_date.hour == 0 &&
                                statement->values[i].value_date.minute == 0 &&
                                statement->values[i].value_date.second == 0 &&
                                statement->values[i].value_date.microsecond == 0)
                            {
                                 packet.size += 5; // [4][yy][m][d]
                            }
                            else
                            {
                                if (statement->values[i].value_date.microsecond == 0)
                                {
                                    packet.size += 8; // [7][yy][m][d][h][m][s]
                                }
                                else
                                {
                                    packet.size += 12; // [11][yy][m][d][h][m][s][uuuu]
                                }
                            }
                        }
                        break;
                    default:
                        /* 
                         * ToDo: for large data use BIND_LONG_DATA
                         */
                        packet.size += db_mysql_calc_lenencstr_size(statement->values[i].size);
                        break;
                    }
                }
            }
        }
    }

    packet.data = (char*)api_alloc(pool, packet.size);

    /*
     * Fill buffer with headers and values
     */

    pos = packet.data;
    *pos++ = COM_STMT_EXECUTE;
    *(int*)pos = statement->id;
    pos += 4;
    *pos++ = 0; // flags
    *(int*)pos = 1; // iteration count
    pos += 4;

    if (statement->num_params > 0)
    {
        for (i = 0; i < (statement->num_params + 7) / 8; ++i)
            pos[i] = 0;

        for (i = 0; i < statement->num_params; ++i)
        {
            *(pos + (i / 8)) |= (statement->values[i].is_null << (i % 8));
        }

        pos += (statement->num_params + 7) / 8;

        *pos++ = statement->params_changed;

        if (statement->params_changed)
        {
            for (i = 0; i < statement->num_params; ++i)
            {
                *(short*)pos = (short)statement->mysql_types[i];
                pos += 2;
            }

            for (i = 0; i < statement->num_params; ++i)
            {
                if (!statement->values[i].is_null)
                {
                    switch (statement->params[i].type) {
                    case DB_TYPE_BOOL:
                    case DB_TYPE_BYTE:
                        *pos++ = statement->values[i].value_byte;
                        packet.size += 1;
                        break;
                    case DB_TYPE_SHORT:
                        *(short*)pos = statement->values[i].value_short;
                        pos += 2;
                        break;
                    case DB_TYPE_INT:
                        *(int*)pos = statement->values[i].value_int;
                        pos += 4;
                        break;
                    case DB_TYPE_FLOAT:
                        *(float*)pos = statement->values[i].value_float;
                        pos += 4;
                        break;
                    case DB_TYPE_INT64:
                        *(int64_t*)pos = statement->values[i].value_int64;
                        pos += 8;
                        break;
                    case DB_TYPE_DOUBLE:
                        *(double*)pos = statement->values[i].value_double;
                        pos += 8;
                        break;
                    case DB_TYPE_TIME:
                        if (statement->values[i].value_time.days == 0 &&
                            statement->values[i].value_time.hours == 0 &&
                            statement->values[i].value_time.minutes == 0 &&
                            statement->values[i].value_time.seconds == 0 &&
                            statement->values[i].value_time.microseconds == 0)
                        {
                            *pos++ = 0; // [0]
                        }
                        else
                        {
                            if (statement->values[i].value_time.microseconds == 0)
                            {
                                // [8][+/-][dddd][h][m][s]
                                *pos++ = 8;
                                *pos++ = statement->values[i].value_time.is_negative;
                                *(int*)pos = statement->values[i].value_time.days; pos+= 4;
                                *pos++ = statement->values[i].value_time.hours;
                                *pos++ = statement->values[i].value_time.minutes;
                                *pos++ = statement->values[i].value_time.seconds;
                            }
                            else
                            {
                                // [12][+/-][dddd][h][m][s][uuuu]
                                *pos++ = 12;
                                *(int*)pos = statement->values[i].value_time.days; pos+= 4;
                                *pos++ = statement->values[i].value_time.hours;
                                *pos++ = statement->values[i].value_time.minutes;
                                *pos++ = statement->values[i].value_time.seconds;
                                *(int*)pos = statement->values[i].value_time.microseconds; pos+= 4;
                            }
                        }
                        break;
                    case DB_TYPE_DATE:
                    case DB_TYPE_DATETIME:
                    case DB_TYPE_TIMESTAMP: 
                        if (statement->values[i].value_date.year == 0 &&
                            statement->values[i].value_date.month == 0 &&
                            statement->values[i].value_date.day == 0 &&
                            statement->values[i].value_date.hour == 0 &&
                            statement->values[i].value_date.minute == 0 &&
                            statement->values[i].value_date.second == 0 &&
                            statement->values[i].value_date.microsecond == 0)
                        {
                            *pos++ = 0; // [0]
                        }
                        else
                        {
                            if (statement->values[i].value_date.hour == 0 &&
                                statement->values[i].value_date.minute == 0 &&
                                statement->values[i].value_date.second == 0 &&
                                statement->values[i].value_date.microsecond == 0)
                            {
                                // [4][yy][m][d]
                                *pos++ = 4;
                                *(short*)pos = statement->values[i].value_date.year; pos+= 2;
                                *pos++ = statement->values[i].value_date.month;
                                *pos++ = statement->values[i].value_date.day;
                            }
                            else
                            {
                                if (statement->values[i].value_date.microsecond == 0)
                                {
                                    // [7][yy][m][d][h][m][s]
                                    *pos++ = 7;
                                    *(short*)pos = statement->values[i].value_date.year; pos+= 2;
                                    *pos++ = statement->values[i].value_date.month;
                                    *pos++ = statement->values[i].value_date.day;
                                    *pos++ = statement->values[i].value_date.hour;
                                    *pos++ = statement->values[i].value_date.minute;
                                    *pos++ = statement->values[i].value_date.second;
                                }
                                else
                                {
                                    // [11][yy][m][d][h][m][s][uuuu]
                                    *pos++ = 11;
                                    *(short*)pos = statement->values[i].value_date.year; pos+= 2;
                                    *pos++ = statement->values[i].value_date.month;
                                    *pos++ = statement->values[i].value_date.day;
                                    *pos++ = statement->values[i].value_date.hour;
                                    *pos++ = statement->values[i].value_date.minute;
                                    *pos++ = statement->values[i].value_date.second;
                                    *(int*)pos = statement->values[i].value_date.microsecond; pos+= 4;
                                }
                            }
                        }
                        break;
                    default:
                        /* 
                         * ToDo: for large data use BIND_LONG_DATA
                         */
                        pos = db_mysql_write_lenencint(pos, statement->values[i].size);
                        memcpy(pos, statement->values[i].value_binary, statement->values[i].size);
                        pos += statement->values[i].size;
                        break;
                    }
                }
            }
        }
    }

    /*
     * Sent command
     */

    code = db_mysql_write(statement->connection, &packet);

    db_mysql_free(statement->connection, &packet);

    if (DB_OK != code)
        return code;

    /*
     * Read result
     */

    code = db_mysql_read_result(statement->connection, statement->id);
    if (DB_OK == code)
    {
        if (statement->connection->result != 0)
            statement->connection->result->statement_id = statement->id;

        if (result != 0)
        {
            *result = statement->connection->result;
        }
        else
        {
            db_mysql_eat_result(statement->connection);
        }
    }

    return code;
}

int db_mysql_statement_close(db_mysql_statement_t* statement)
{
    api_pool_t* pool = api_pool_default(statement->connection->session->base.loop);
    char command[4 + 5];
    int code = DB_OK;

    /*
     * Send COM_STMT_CLOSE command
     */

    *(int*)command = 5;
    command[3] = 0; // reset sequence
    command[4] = COM_STMT_CLOSE;
    *(int*)(command + 5) = statement->id;

    if (4 + 5 != api_stream_write(&statement->connection->tcp.stream, command, 4 + 5))
    {
        statement->connection->undefined = 1;
        code = DB_UNAVAILABLE;
    }

    db_mysql_statement_free(pool, statement);

    return code;
}
