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

/*
 * Called from db_pool if there is no cached connection
 */
int db_mysql_connection_create(db_mysql_session_t* session, db_mysql_connection_t** connection)
{
    api_pool_t* pool = api_pool_default(session->base.loop);
    db_mysql_connection_t* con;
    int error = 0;
    db_mysql_packet_t packet;
    db_mysql_status_t status;
    uint64_t offset = 0;
    char* handshake;
    int length_username;
    int length_schema;
    int length;
    int replied;
    char* reply;
    char auth[20];
    char buf1[2 * SHA1_HASH_SIZE];
    char buf2[SHA1_HASH_SIZE];
    char sha1_password[SHA1_HASH_SIZE];
    SHA1Context sha1;
    int* it1;
    int* it2;

    *connection = 0;

    /*
     * Check cached statuses
     */

    if (session->server.low_version)
        return DB_NOT_SUPPORTED;

    if (session->server.auth_failed)
        return DB_FAILED;

    /*
     * First time connecting or last connect was succeded
     */

    con = (db_mysql_connection_t*)api_calloc(pool, sizeof(*con));
    con->session = session;

    error = api_tcp_connect(&con->tcp, session->base.loop, session->ip, session->port, session->base.connect_timeout);
    if (API_OK != error)
    {
        api_free(pool, sizeof(*con), con);
        return DB_CONNECT_FAILED;
    }

    con->tcp.stream.read_timeout = session->base.timeout;
    con->tcp.stream.write_timeout = session->base.timeout;

    /*
     * Parse server handshake
     */

    error = db_mysql_read(con, &packet);
    if (DB_OK != error)
    {
        api_stream_close(&con->tcp.stream);
        api_free(pool, sizeof(*con), con);
        return DB_CONNECT_FAILED;
    }

    handshake = packet.data;

    con->session->server.version = *handshake;
    if (con->session->server.version < 10)
    {
        /*
         * Supported versions are 4.1 and above
         */
        con->session->server.low_version = 1;
        db_mysql_free(con, &packet);
        api_stream_close(&con->tcp.stream);
        api_free(pool, sizeof(*con), con);
        return DB_NOT_SUPPORTED;
    }

    /*
     * Save first 8 byte of 20 byte length random data
     */
    offset = 1 + strlen(handshake + 1) + 1;
    memcpy(auth, handshake + offset + 4, 8);
    offset += 4 + 9;

    con->session->server.capabilities = *(unsigned short*)(handshake + offset);
    offset += 2;

    if (packet.size > offset)
    {
        con->session->server.charset = *(handshake + offset);
        offset += (1 + 2);
        con->session->server.capabilities |= *(unsigned short*)(handshake + offset);
        offset += 2;

        offset += 1;
        offset += 10;

        /*
         * Append last 12 bytes of random data
         */
        memcpy(auth + 8, handshake + offset, 12);
    }

    db_mysql_free(con, &packet);

    /*
     * Send credentials
     */

    length_username = strlen(con->session->username) + 1;
    length_schema = strlen(con->session->schema) + 1;

    length  = 4 // client capabilities
            + 4 // max packet length
            + 1 // charset
            + 23 // reserved
            + length_username
            + 1 // length sha1
            + SHA1_HASH_SIZE
            + length_schema
            + sizeof("mysql_native_password");

    reply = (char*)api_calloc(pool, length + 4);

    *(int*)reply = length;
    reply[3] = 1; // packet sequence

    *((int*)reply + 1) = 
        (/*CLIENT_FOUND_ROWS |*/ CLIENT_LONG_FLAG | CLIENT_CONNECT_WITH_DB |
        CLIENT_IGNORE_SPACE | CLIENT_PROTOCOL_41 | CLIENT_IGNORE_SIGPIPE | CLIENT_TRANSACTIONS |
        CLIENT_SECURE_CONNECTION | CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS | CLIENT_PS_MULTI_RESULTS);

    *(reply + 4 + 4 + 4) = 33; // utf8
    strcpy(reply + 4 + 4 + 4 + 1 + 23, con->session->username);
    *(reply + 4 + 4 + 4 + 1 + 23 + length_username) = SHA1_HASH_SIZE;


    // SHA1(password) XOR SHA1("20-bytes random data from server" <concat> SHA1(SHA1(password)))

    memcpy(buf1, auth, 20);

    // SHA1(password)
    SHA1Init(&sha1);
    SHA1Update(&sha1, con->session->password, strlen(con->session->password));
    SHA1Final(&sha1, (uint8_t*)sha1_password);

    // SHA1(SHA1(password))
    SHA1Init(&sha1);
    SHA1Update(&sha1, sha1_password, SHA1_HASH_SIZE);
    SHA1Final(&sha1, (uint8_t*)buf1 + SHA1_HASH_SIZE);

    // SHA1("20-bytes random data from server" <concat> SHA1(SHA1(password)))
    SHA1Init(&sha1);
    SHA1Update(&sha1, buf1, 20 + SHA1_HASH_SIZE);
    SHA1Final(&sha1, (uint8_t*)buf2);

    it1 = (int*)sha1_password;
    it2 = (int*)buf2;

    // XOR
    it1[0] = it1[0] ^ it2[0];
    it1[1] = it1[1] ^ it2[1];
    it1[2] = it1[2] ^ it2[2];
    it1[3] = it1[3] ^ it2[3];
    it1[4] = it1[4] ^ it2[4];

    memcpy(reply + 4 + 4 + 4 + 1 + 23 + length_username + 1,  sha1_password, SHA1_HASH_SIZE);
    strcpy(reply + 4 + 4 + 4 + 1 + 23 + length_username + 1 + SHA1_HASH_SIZE, con->session->schema);
    strcpy(reply + 4 + 4 + 4 + 1 + 23 + length_username + 1 + SHA1_HASH_SIZE + length_schema, "mysql_native_password");

    replied = api_stream_write(&con->tcp.stream, reply, length + 4);

    api_free(pool, length + 4, reply);

    if (replied != length + 4)
    {
        api_stream_close(&con->tcp.stream);
        api_free(pool, sizeof(*con), con);
        return DB_UNAVAILABLE;
    }

    /*
     * Check auth status
     */

    db_mysql_status_read(con, &status);

    if (DB_OK != status.code)
    {
        session->server.auth_failed = 1;

        /*
         * Save error to session
         */
        db_error_override(pool, &session->base.error, &status.err);
        status.err.message = 0;

        db_mysql_status_free(pool, &status);
        api_stream_close(&con->tcp.stream);
        api_free(pool, sizeof(*con), con);
        return DB_FAILED;
    }

    db_mysql_status_free(pool, &status);

    *connection = con;
    return DB_OK;
}

int db_mysql_connection_open(db_mysql_session_t* session, db_mysql_connection_t** connection)
{
    /*
     * Get from pool if exist
     */
    return db_pool_open_connection((db_session_t*)session, (db_connection_t**)connection);
}

int db_mysql_connection_error(db_mysql_connection_t* connection, db_error_t* error)
{
    memcpy(error, &connection->error, sizeof(*error));

    return DB_OK;
}

int db_mysql_connection_query(db_mysql_connection_t* connection, const char* sql, db_mysql_result_t** result)
{
    int sql_length = strlen(sql);
    char command[5];
    int code;

    if (result)
        *result = 0;

    /*
     * Eat pending resultsets
     */
    db_mysql_eat_result(connection);

    /*
     * Send COM_QUERY command
     */

    *(int*)command = 1 /* COM_QUERY */
                   + sql_length;

    command[3] = 0; // reset sequence
    command[4] = COM_QUERY;

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
     * Read result
     */

    code = db_mysql_read_result(connection, 0);
    if (DB_OK == code)
    {
        if (result != 0)
        {
            *result = connection->result;
        }
        else
        {
            db_mysql_eat_result(connection);
        }
    }

    return code;
}

int db_mysql_connection_affected(db_mysql_connection_t* connection, uint64_t* affected)
{
    *affected = connection->affected;
    return DB_OK;
}

int db_mysql_connection_insert_id(db_mysql_connection_t* connection, uint64_t* insert_id)
{
    *insert_id = connection->insert_id;
    return DB_OK;
}

int db_mysql_connection_begin(db_mysql_connection_t* connection)
{
    /*
     * Call through iface, in case when iface was hooked
     */
    return connection->session->base.iface.connection.query((db_connection_t*)connection, "START TRANSACTION", 0 /* skip results ? */);
}

int db_mysql_connection_commit(db_mysql_connection_t* connection)
{
    /*
     * Call through iface, in case when iface was hooked
     */
    return connection->session->base.iface.connection.query((db_connection_t*)connection, "COMMIT", 0 /* skip results ? */);
}

int db_mysql_connection_rollback(db_mysql_connection_t* connection)
{
    /*
     * Call through iface, in case when iface was hooked
     */
    return connection->session->base.iface.connection.query((db_connection_t*)connection, "ROLLBACK", 0 /* skip results ? */);
}

int db_mysql_connection_close(db_mysql_connection_t* connection)
{
    db_error_cleanup(api_pool_default(connection->session->base.loop), &connection->error);

    if (connection->undefined)
    {
        /*
         * Dont reuse broken connections
         */
        return db_mysql_connection_destroy(connection);
    }
    else
    {
        /*
         * Eat pending resultsets
         */
        db_mysql_eat_result(connection);

        /*
         * Put back into pool
         */
        return db_pool_close_connection((db_connection_t*)connection);
    }
}

/*
 * Called from db_pool if cache is full
 */
int db_mysql_connection_destroy(db_mysql_connection_t* connection)
{
    api_pool_t* pool = api_pool_default(connection->session->base.loop);
    char command[5];

    db_error_cleanup(pool, &connection->error);

    if (!connection->undefined)
    {
        memset(command, 0, 5);
        command[0] = 1; // packet size
        command[4] = COM_QUIT;

        if (5 == api_stream_write(&connection->tcp.stream, command, 5))
        {
            /*
             * Is there a reason for waiting status packet here ?
             */
        }
    }

    /*
     * Free memory
     */
    if (connection->result)
        db_mysql_result_close(connection->result);

    api_stream_close(&connection->tcp.stream);
    api_free(pool, sizeof(*connection), connection);

    return DB_OK;
}
