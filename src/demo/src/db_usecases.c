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

/*
 * db access demo with error checkings
 */

#include <stdio.h>

#include "../../db/include/db.h"

db_session_t* session;

void print_columns(db_column_t* columns, int count)
{
    int i;

    for (i = 0; i < count; i++)
        printf("%14s", columns[i].name);

    printf("\r\n");
}

void print_row(db_column_t* columns, db_value_t* row, int fields)
{
    int i;
    char buf[100];
    db_date_t* date;
    db_time_t* time;

    for (i = 0; i < fields; i++)
    {
        if (row[i].is_null)
            printf("%14s", "NULL");
        else
        {
            switch (columns[i].type)
            {
            case DB_TYPE_BOOL:
            case DB_TYPE_BYTE:
            case DB_TYPE_SHORT:
            case DB_TYPE_INT:
            case DB_TYPE_INT64:
                printf("%14d", row[i].value_int64); break;
            case DB_TYPE_FLOAT:
                printf("%14f", (double)row[i].value_float); break;
            case DB_TYPE_DOUBLE:
                printf("%14f", row[i].value_double); break;
            case DB_TYPE_TIME:
                time = &row[i].value_time;
                sprintf(buf, "%s%02dd %02dh %02dm %02ds", time->is_negative ? "-" : "", time->days, time->hours, time->minutes, time->seconds);
                printf("%14s", buf);
                break;
            case DB_TYPE_DATE:
            case DB_TYPE_DATETIME:
            case DB_TYPE_TIMESTAMP:
                date = &row[i].value_date;
                sprintf(buf, "%d-%02d-%02d %02d:%02d:%02d", date->year, date->month, date->day, date->hour, date->minute, date->second);
                printf("%14s", buf);
                break;
            case DB_TYPE_STRING:
                printf("%14s", row[i].value_string);
            case DB_TYPE_BINARY:
                break;
            default:
                printf("%14s", "?");
                break;
            }
        }
    }

    printf("\r\n");
}

void print_result(db_result_t* result, int rows_to_fetch_per_call)
{
    db_column_t* columns;
    db_value_t** rows;
    int num_columns;
    int num_rows;
    int i;

    /*
     * For each result set
     */
    while (db_result_fetch_columns(result, &columns, &num_columns) == DB_OK)
    {
        print_columns(columns, num_columns);

        num_rows = rows_to_fetch_per_call;

        /*
         * For each row in result set
         */
        while (db_result_fetch_rows(result, &rows, &num_rows) == DB_OK && num_rows > 0)
        {
            for (i = 0; i < num_rows; ++i)
                print_row(columns, rows[i], num_columns);
        }
    }
}

void db_uc_errors()
{
    db_connection_t* connection;
    db_statement_t* statement;
    db_result_t* result;
    db_error_t error;
    int code;

    printf("\r\n\r\nusecase error checking\r\n");

    code = db_connection_open(session, &connection);
    if (DB_OK == code)
    {
        /*
         * There is no table named `ity`
         */
        if (DB_OK == db_statement_prepare(connection, "Select * From `ity` Where `ID` > ? limit 10", &statement))
        {
            code = db_statement_bind_string(statement, /* index*/ 0, /* value */ "9");
            if (DB_OK == code)
            {
                if (DB_OK == db_statement_exec(statement, &result))
                {
                    print_result(result, 10);

                    db_result_close(result);
                }
                else // db_statement_exec
                {
                    db_connection_error(connection, &error);
                    printf("db_statement_exec: %d, #%s - %s\r\n", error.code, error.state, error.message); 
                }
            }
            else
            {
                switch (code) {
                    case DB_OUT_OF_INDEX: printf("invalid parameter index\r\n"); break;
                    case DB_MISMATCH: printf("parameter type not compatible\r\n"); break;
                    case DB_TOO_LONG: printf("parameter value too large to fit\r\n"); break;
                    default: printf("somethnig failed\r\n"); break;
                }
            }

            db_statement_close(statement);
        }
        else // db_statement_prepare
        {
            db_connection_error(connection, &error);
            printf("db_statement_prepare: %d, #%s - %s\r\n", error.code, error.state, error.message);
        }

        db_connection_close(connection);
    }
    else // db_connection_open
    {
        printf("db_connection_open: ");

        if (DB_FAILED == code)
        {
            db_session_error(session, &error);
            printf("%d, #%s - %s\r\n", error.code, error.state, error.message);
        }
        else
        {
            switch (code) {
                case DB_NOT_SUPPORTED: printf("server or version not supported\r\n"); break;
                case DB_CONNECT_FAILED: printf("connect failed\r\n"); break;
                case DB_UNAVAILABLE: printf("server unavailable\r\n"); break;
                default: printf("somethnig failed\r\n"); break;
            }
        }
    }
}

void db_uc_query()
{
    db_connection_t* connection;
    db_statement_t* statement;
    db_result_t* result;

    printf("\r\n\r\nusecase query\r\n");

    if (db_connection_open(session, &connection) == DB_OK)
    {
        if (db_connection_query(connection, "Select * From `country` limit 10", &result) == DB_OK)
        {
            print_result(result, 0 /* fetch all rows in single call */);

            db_result_close(result);
        }

        db_connection_close(connection);
    }
}

void db_uc_query_multiple()
{
    db_connection_t* connection;
    db_statement_t* statement;
    db_result_t* result;

    printf("\r\n\r\nusecase query with multiple resultset\r\n");

    if (db_connection_open(session, &connection) == DB_OK)
    {
        if (db_connection_query(connection, 
                "Select * From `city` limit 1;"
                "Select `Code`, `Name` From `country` limit 1", &result) == DB_OK)
        {
            print_result(result, 10 /* fetch 10 rows per call */);

            db_result_close(result);
        }

        db_connection_close(connection);
    }
}

void db_uc_stored_procedure()
{
    db_connection_t* connection;
    db_statement_t* statement;
    db_result_t* result;

    printf("\r\n\r\nusecase stored procedure\r\n");

    if (db_connection_open(session, &connection) == DB_OK)
    {
        if (db_connection_query(connection, "call sp_test()", &result) == DB_OK)
        {
            print_result(result, 0 /* fetch all rows in single call */);

            db_result_close(result);
        }

        db_connection_close(connection);
    }
}

void db_uc_exec()
{
    db_connection_t* connection;
    db_statement_t* statement;
    db_result_t* result;

    printf("\r\n\r\nusecase statement\r\n");

    if (DB_OK == db_connection_open(session, &connection))
    {
        if (DB_OK == db_statement_prepare(connection, "Select * From `city` Where `ID` > ? limit 10", &statement))
        {
            db_statement_bind_string(statement, /* index*/ 0, /* value */ "9");
            if (DB_OK == db_statement_exec(statement, &result))
            {
                print_result(result, 10 /* fetch 10 rows per call */);

                db_result_close(result);
            }

            db_statement_close(statement);
        }

        db_connection_close(connection);
    }
}

void db_uc_blob()
{
    db_connection_t* connection;
    db_statement_t* statement;
    uint64_t affected;
    char blob[] = "sdlfjdsjhngjkfdghkfjhdfkgdkjghdkhdighdfghdiughdiughdfiughdiughdfiughdiughdghdiuhgdiughdiughidhgdfig";

    printf("\r\n\r\nusecase sending blob data\r\n");

    if (DB_OK == db_connection_open(session, &connection))
    {
        if (DB_OK == db_statement_prepare(connection, "Update `tbl` Set `blob` = ? Where `ID` = 1", &statement))
        {
            /*
             * Send blob
             */
            db_statement_bind_blob(statement, /* index*/ 0, /* value */ blob, sizeof(blob) - 1);

            /*
             * Append blob
             */
            db_statement_bind_blob(statement, /* index*/ 0, /* value */ blob, sizeof(blob) - 1);

            db_statement_exec(statement, 0 /* dont need resultset */);

            db_connection_affected(connection, &affected);

            printf("affected %d\r\n", affected);

            db_statement_close(statement);
        }

        db_connection_close(connection);
    }
}

void db_uc_update()
{
    db_connection_t* connection;
    db_statement_t* statement;
    uint64_t affected;

    printf("\r\n\r\nusecase get affected count\r\n");

    if (DB_OK == db_connection_open(session, &connection))
    {
        if (DB_OK == db_statement_prepare(connection, "Update `city` Set `id` = ? Where `ID` = ?", &statement))
        {
            db_statement_bind_string(statement, /* index*/ 0, /* value */ "9");
            db_statement_bind_string(statement, /* index*/ 1, /* value */ "9");
            db_statement_exec(statement, 0 /* dont need resultset */);

            db_connection_affected(connection, &affected);

            printf("affected %d\r\n", affected);

            db_statement_close(statement);
        }

        db_connection_close(connection);
    }
}

void db_uc_insert()
{
    db_connection_t* connection;
    db_statement_t* statement;
    uint64_t insert_id;

    printf("\r\n\r\nusecase get last insert id\r\n");

    if (DB_OK == db_connection_open(session, &connection))
    {
        if (DB_OK == db_statement_prepare(connection, 
            "Insert into `city` (`Name`, `CountryCode`, `District`, `Population`) Values (?,?,?,?)", &statement))
        {
            db_statement_bind_string(statement, /* index*/ 0, /* value */ "Yerevan");
            db_statement_bind_string(statement, /* index*/ 1, /* value */ "Armenia");
            db_statement_bind_string(statement, /* index*/ 2, /* value */ "Yerevan");
            db_statement_bind_string(statement, /* index*/ 3, /* value */ "1500000");

            /*
             * Replace bound parameter
             */
            db_statement_bind_string(statement, /* index*/ 1, /* value */ "ARM");
            
            db_statement_exec(statement, 0 /* dont need resultset */);

            db_connection_insert_id(connection, &insert_id);

            printf("last insert id %d\r\n", insert_id);

            db_statement_close(statement);
        }

        db_connection_close(connection);
    }
}

void db_uc_transaction()
{
    db_connection_t* connection;
    db_statement_t* statement;

    printf("\r\n\r\nusecase transactions\r\n");

    if (DB_OK == db_connection_open(session, &connection))
    {
        db_connection_begin(connection);

        /*
         * Are you sure ? :)
         */
        //db_connection_query(connection, "Delete * From `City`", 0);

        db_connection_rollback(connection);

        db_connection_close(connection);
    }
}

void db_run_usecases(api_loop_t* loop, void* arg)
{
    db_engine_t engine;
    db_error_t error;
    int code;
    
    /*
     * Configure as MySQL
     */
    engine.type = DB_ENGINE_MYSQL;
    engine.connect_timeout = 500;
    engine.timeout = 10 * 1000;
    engine.pool_size = 10;
    engine.db.mysql.server = "127.0.0.1";
    engine.db.mysql.port = 3306;
    engine.db.mysql.username = "MySQLUser";
    engine.db.mysql.password = "sasasa";
    engine.db.mysql.schema = "world";
    engine.db.mysql.flags = 0;

    /*
     * Start MySQL session
     */
    code = db_session_start(loop, &engine, &session);
    if (DB_OK != code)
    {
        /*
         * On failure dump error
         */

        printf("Failed to start session: ");

        if (DB_FAILED == code)
        {
            db_session_error(session, &error);
            printf("%d, #%s - %s\r\n", error.code, error.state, error.message);
        }
        else
        {
            switch (code) {
                case DB_NOT_SUPPORTED: printf("server or version not supported\r\n"); break;
                case DB_CONNECT_FAILED: printf("connect failed\r\n"); break;
                case DB_UNAVAILABLE: printf("server unavailable\r\n"); break;
                default: printf("somethnig failed\r\n"); break;
            }
        }

        return;
    }

    db_uc_errors();
    db_uc_query();
    db_uc_stored_procedure();
    db_uc_query_multiple();
    db_uc_exec();
    db_uc_blob();
    db_uc_update();
    db_uc_insert();
    db_uc_transaction();
}

int main(int argc, char *argv[])
{
    int code = 0;
    session = 0;

    api_init();

    if (API_OK != api_loop_run(db_run_usecases, 0, 50 * 1024))
        code = 1;

    /*
     * Close session and dispose pooled connections on app exit
     */
    if (session != 0)
        db_session_close(session);

    return code;
}