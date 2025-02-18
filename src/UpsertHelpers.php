<?php declare(strict_types=1);

namespace AP\Mysql;

use AP\Mysql\Connect\ConnectInterface;
use Generator;
use UnexpectedValueException;

class UpsertHelpers
{
    public static function prepareRow(ConnectInterface $connect, array $row): string
    {
        $names  = "";
        $values = "";
        foreach ($row as $k => $v) {
            $names  .= ",`$k`";
            $values .= ",{$connect->escape($v)}";
        }
        return '(' . substr($names, 1) . ') VALUE (' . substr($values, 1) . ')';
    }

    public static function prepareOnDupKeyUpdate(ConnectInterface $connect, array $update): string
    {
        $update_statement = "";
        foreach ($update as $k => $v) {
            $update_statement .= ",`$k`={$connect->escape($v)}";
        }
        return 'ON DUPLICATE KEY UPDATE ' . substr($update_statement, 1);
    }

    public static function prepareCols(ConnectInterface $connect, array $cols): string
    {
        $names = "";
        foreach ($cols as $col) {
            $names .= ",`$col`";
        }
        return '(' . substr($names, 1) . ')';
    }

    private static function rowNamesHash($keys): string
    {
        return implode(":", $keys);
    }

    public static function bulkRunner(
        ConnectInterface $connect,
        string           $query_begin,
        string           $query_end,
        string           $table,
        array            $rows,
        int              $batch,
        array            $addToRow = [],
        bool             $ignore = false,
        string           $partition = "",
        bool             $deepValidation = true
    ): Generator
    {
        if (empty($rows)) {
            throw new UnexpectedValueException('rows must have data');
        }

        // reset($rows); // TEST PERFORMANCE
        $rowNames = array_keys($rows[key($rows)]);

        if (empty($addToRow)) {
            $allNames = $rowNames;
        } else {
            if ($deepValidation) {
                foreach ($rowNames as $rowName) {
                    if (key_exists($rowName, $addToRow)) {
                        throw new UnexpectedValueException("rows have duplicate with addToRow name: $rowName");
                    }
                }
            }
            $allNames = array_merge(array_keys($addToRow), $rowNames);
        }

        $query_begin .=
            ($ignore ? ' IGNORE' : '') .
            " `$table`" .
            ($partition ? " PARTITION ($partition)" : '') .
            '(`' . implode("`,`", $allNames) . '`) VALUE ';

        $num         = 0;
        $blockNum    = 0;
        $rowNamesStr = $deepValidation ? self::rowNamesHash($rowNames) : "";

        $all_values = '';

        foreach ($rows as $row) {
            $num++;
            $blockNum++;
            if ($deepValidation) {
                if ($rowNamesStr != self::rowNamesHash(array_keys($row))) {
                    throw new UnexpectedValueException(
                        "invalid rows[$num], keys no match: $rowNamesStr and " .
                        self::rowNamesHash(array_keys($row))
                    );
                }
            }

            $row_values = "";
            foreach ($addToRow as $v) {
                $row_values .= ",{$connect->escape($v)}";
            }
            foreach ($row as $v) {
                $row_values .= ",{$connect->escape($v)}";
            }
            $all_values .= ',(' . substr($row_values, 1) . ')';

            if ($blockNum == $batch) {
                yield $query_begin . substr($all_values, 1) . $query_end;
                $all_values = '';
                $blockNum   = 0;
            }
        }

        if ($blockNum > 0) {
            yield $query_begin . substr($all_values, 1) . $query_end;
        }
    }
}