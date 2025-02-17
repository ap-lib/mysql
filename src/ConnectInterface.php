<?php declare(strict_types=1);

namespace AP\Mysql;

use mysqli_result;
use mysqli_sql_exception;

interface ConnectInterface
{
    /**
     * Performs a query on the database
     *
     * @param string $query The query string. The data must be properly formatted, and all strings must be
     *                              escaped using the **mysqli_real_escape_string** function.
     *
     * @return mysqli_result|true   For successful queries which produce a result set, such as SELECT, SHOW, DESCRIBE
     *                              or EXPLAIN **mysqli_query** will return a **mysqli_result** object.
     *                              For other successful queries **mysqli_query** will return true.
     *
     * @throws mysqli_sql_exception For errors
     */
    public function exec(string $query): mysqli_result|true;

    public function escape(mixed $value): string;

    public function lastInsertId(): int;

    public function lastAffectedRows(): int;
}