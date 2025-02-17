<?php declare(strict_types=1);

namespace AP\Mysql;

use mysqli_result;
use UnexpectedValueException;

class ConnectDebug implements ConnectInterface
{
    public function exec(string $query): mysqli_result|true
    {
        return true;
    }

    public function escape(mixed $value): string
    {
        if (is_string($value)) {
            return '\'' . addslashes($value) . '\'';
        }
        if (is_bool($value)) {
            return $value ? '1' : '0';
        }
        if (is_null($value)) {
            return 'null';
        }
        if (is_int($value) || is_float($value)) {
            return (string)$value;
        }

        throw new UnexpectedValueException('this value can\'t be escaped');
    }

    public function lastInsertId(): int
    {
        return 0;
    }

    public function lastAffectedRows(): int
    {
        return 0;
    }
}