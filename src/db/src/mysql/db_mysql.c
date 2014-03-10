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

#include "../3rdparty/shaman/sha1.h"
#include "db_mysql.h"

int db_mysql_read(db_mysql_connection_t* connection, db_mysql_packet_t* packet)
{
    int header;
    api_pool_t* pool = api_pool_default(connection->session->base.loop);

    if (connection->undefined)
        return DB_UNAVAILABLE;

    if (4 > api_stream_read_exact(&connection->tcp.stream, (char*)&header, 4))
    {
        connection->undefined = 1;
        return DB_UNAVAILABLE;
    }

    packet->size = header & 0x00ffffff;
    packet->sequence = header >> 24;
    packet->data = (char*)api_alloc(pool, packet->size);

    if (packet->size > api_stream_read_exact(&connection->tcp.stream, packet->data, packet->size))
    {
        api_free(pool, packet->size, packet->data);
        connection->undefined = 1;
        return DB_UNAVAILABLE;
    }

    return DB_OK;
}

int db_mysql_write(db_mysql_connection_t* connection, db_mysql_packet_t* packet)
{
    int header = packet->size | (packet->sequence << 24);

    if (connection->undefined)
    {
        connection->undefined = 1;
        return DB_UNAVAILABLE;
    }

    if (4 > api_stream_write(&connection->tcp.stream, (char*)&header, 4))
    {
        connection->undefined = 1;
        return DB_UNAVAILABLE;
    }

    if (packet->size > api_stream_write(&connection->tcp.stream, packet->data, packet->size))
    {
        connection->undefined = 1;
        return DB_UNAVAILABLE;
    }

    return DB_OK;
}

void db_mysql_free(db_mysql_connection_t* connection, db_mysql_packet_t* packet)
{
    api_pool_t* pool = api_pool_default(connection->session->base.loop);

    if (packet->size > 0)
    {
        api_free(pool, packet->size, packet->data);
        packet->size = 0;
    }
}

uint64_t db_mysql_read_lenencint(char* buffer, uint64_t* count)
{
    uint64_t value = (unsigned char)*buffer;
    *count = 0;

    if (value < 0xfb)
    {
        *count = 1;
    }
    else
    if (value == 0xfc)
    {
        value = *(unsigned short*)(buffer + 1);
        *count = 3;
    }
    else
    if (value == 0xfd)
    {
        value = 4 + 0x00ffffff & *(int*)(buffer + 1);
        *count = 4;
    }
    else
    if (value == 0xfe)
    {
        value = 9 + *(uint64_t*)(buffer + 1);
        *count = 9;
    }
    else
    {
        /* we should not be here */
    }
    
    return value;
}

char* db_mysql_write_lenencint(char* buffer, uint64_t value)
{
    if (value < 0xfb)
    {
        *buffer++ = (char)value;
    }
    else
    if (value < 0x10000) // 2^16
    {
        *buffer++ = 0xfc;
        *(short*)buffer = (short)value;
        buffer += 2;
    }
    else
    if (value < 0x1000000) // 2^24
    {
        *buffer++ = 0xfd;
        *(short*)buffer = (short)value;
        buffer += 2;
        *buffer++ = *((char*)&value + 2);
    }
    else
    {
        *buffer++ = 0xfe;
        *(uint64_t*)buffer = value;
        buffer += 8;
    }
    
    return buffer;
}

uint64_t db_mysql_read_lenencstr(api_pool_t* pool, char* buffer, char** str, uint64_t* length)
{
    uint64_t count;
    uint64_t len;

    len = db_mysql_read_lenencint(buffer, &count);
    *str = (char*)api_alloc(pool, (size_t)len + 1);
    memcpy(*str, buffer + count, len);
    (*str)[len] = 0;

    if (length)
        *length = len;

    return count + len;
}

uint64_t db_mysql_skip_lenencstr(char* buffer)
{
    uint64_t value;
    uint64_t pos;

    value = db_mysql_read_lenencint(buffer, &pos);

    return pos + value;
}

uint64_t db_mysql_calc_lenencstr_size(uint64_t length)
{
    if (length < 0xfb)
        return 1 + length;

    if (length < 0x10000) // < 2^16
        return 1 + 2 + length;

    if (length < 0x1000000) // 2^24
        return 1 + 3 + length;

    return 1 + 8 + length;
}

void db_mysql_parse_error(api_pool_t* pool, db_mysql_packet_t* packet, db_error_t* error)
{
    unsigned int temp;
    db_error_cleanup(pool, error);

    temp = 1; // code
    error->code = *(unsigned short*)(packet->data + temp);
    temp += 3; // error code + '#' the sql-state marker
    memcpy(error->state, packet->data + temp, 5);
    error->state[5] = 0;
    temp += 5;

    if (temp < packet->size)
    {
        error->message = (char*)api_alloc(pool, packet->size - temp + 1);
        memcpy(error->message, packet->data + temp, packet->size - temp);
        error->message[packet->size - temp] = 0;
    }
    else
    {
        error->message = 0;
    }
}

int db_mysql_status_read(db_mysql_connection_t* connection, db_mysql_status_t* status)
{
    api_pool_t* pool = api_pool_default(connection->session->base.loop);
    db_mysql_packet_t packet;
    uint64_t temp;
    uint64_t pos = 0;
    int error;

    status->code = DB_FAILED;

    error = db_mysql_read(connection, &packet);
    if (DB_OK != error)
        return error;

    if (PACKET_IS_OK(packet))
    {
        status->code = PACKET_OK;

        temp = 1; // code
        status->ok.affected_rows = db_mysql_read_lenencint(packet.data + temp, &pos);
        temp += pos;
        status->ok.last_insert_id = db_mysql_read_lenencint(packet.data + temp, &pos);
        temp += pos;
        status->ok.flags = *(unsigned short*)(packet.data + temp);
        status->ok.warnings = *(unsigned short*)(packet.data + temp + 2);
        temp += 4;

        if (temp < packet.size)
        {
            status->ok.info = (char*)api_alloc(pool, packet.size - temp + 1);
            memcpy(status->ok.info, packet.data + temp, packet.size - temp);
            status->ok.info[packet.size - temp] = 0;
        }
        else
        {
            status->ok.info = 0;
        }
    }
    else if (PACKET_IS_EOF(packet))
    {
        status->code = PACKET_EOF;

        temp = 1; // code
        status->eof.warnings = *(unsigned short*)(packet.data + temp);
        status->eof.flags = *(unsigned short*)(packet.data + temp + 2);
    }
    else if (PACKET_IS_ERROR(packet))
    {
        status->code = PACKET_ERROR;

        temp = 1; // code
        status->err.code = *(unsigned short*)(packet.data + temp);
        temp += 3; // error code + '#' the sql-state marker
        memcpy(status->err.state, packet.data + temp, 5);
        status->err.state[5] = 0;
        temp += 5;

        if (temp < packet.size)
        {
            status->err.message = (char*)api_alloc(pool, packet.size - temp + 1);
            memcpy(status->err.message, packet.data + temp, packet.size - temp);
            status->err.message[packet.size - temp] = 0;
        }
        else
        {
            status->err.message = 0;
        }
    }
    else
    {
        /*
         * If we here then something went wrong
         */

        connection->undefined = 1;
        status->code = DB_UNKNOWN;
        error = DB_UNKNOWN;
    }

    db_mysql_free(connection, &packet);
    return error;
}

void db_mysql_status_free(api_pool_t* pool, db_mysql_status_t* status)
{
    switch (status->code)
    {
    case PACKET_OK:
        if (status->ok.info != 0)
            api_free(pool, strlen(status->ok.info) + 1, status->ok.info);
        status->ok.info = 0;
        break;
    case PACKET_ERROR:
        if (status->err.message != 0)
            api_free(pool, strlen(status->err.message) + 1, status->err.message);
        status->err.message = 0;
        break;
    }
}

int db_mysql_detect_type(unsigned char mysql_type)
{
    switch (mysql_type)
    {
    case MYSQL_TYPE_TINY: return DB_TYPE_BYTE;
    case MYSQL_TYPE_SHORT: return DB_TYPE_SHORT;
    case MYSQL_TYPE_LONG: return DB_TYPE_INT;
    case MYSQL_TYPE_FLOAT: return DB_TYPE_FLOAT;
    case MYSQL_TYPE_DOUBLE: return DB_TYPE_DOUBLE;
    case MYSQL_TYPE_TIMESTAMP: return DB_TYPE_TIMESTAMP;
    case MYSQL_TYPE_LONGLONG: return DB_TYPE_INT64;
    case MYSQL_TYPE_INT24: return DB_TYPE_INT;
    case MYSQL_TYPE_DATE: return DB_TYPE_DATE;
    case MYSQL_TYPE_TIME: return DB_TYPE_TIME;
    case MYSQL_TYPE_DATETIME: return DB_TYPE_DATETIME;
    case MYSQL_TYPE_YEAR: return DB_TYPE_SHORT;

    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_STRING:
        return DB_TYPE_STRING;
    }

    /*
     * All other or yet not known types represent as binary
     */
    return DB_TYPE_BINARY;
}

/*
 * Tryes to read first result packet after query, or exec.
 * If resultset available initializes connection.result field,
 * Else leaves it null
 */
int db_mysql_read_result(db_mysql_connection_t* connection, int statement_id)
{
    api_pool_t* pool = api_pool_default(connection->session->base.loop);
    db_mysql_result_t* result;
    db_mysql_packet_t packet;
    uint64_t pos = 0;
    int code = 0;

    /*
     * Eat last result
     */
    code = db_mysql_eat_result(connection);
    if (DB_OK != code)
        return code;

    /*
     * read either ERR, OK or columns count
     */

    code = db_mysql_read(connection, &packet);
    if (DB_OK != code)
        return code;

    else if (PACKET_IS_ERROR(packet))
    {
        /*
         * On failure, set error status in connection and exit
         */
        db_mysql_parse_error(pool, &packet, &connection->error);
        db_mysql_free(result->connection, &packet);
        return DB_FAILED;
    }

    if (PACKET_IS_OK(packet))
    {
        /*
         * On ok, fill up affected and insert_id properties
         */
        connection->affected = db_mysql_read_lenencint(packet.data + 1, &pos);
        connection->insert_id = db_mysql_read_lenencint(packet.data + 1 + pos, &pos);

        db_mysql_free(connection, &packet);
        return DB_OK;
    }
    else
    {
        /*
         * Read Columns Count
         */
        connection->result = (db_mysql_result_t*)api_calloc(pool, sizeof(*result));
        connection->result->connection = connection;
        connection->result->statement_id = statement_id;
        connection->result->num_columns = (int)db_mysql_read_lenencint(packet.data, &pos);
        db_mysql_free(connection, &packet);

        return DB_OK;
    }
}