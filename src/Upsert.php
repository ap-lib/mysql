<?php declare(strict_types=1);

namespace AP\Mysql;

use UnexpectedValueException;

readonly class Upsert
{
    public function __construct(private ConnectInterface $connect)
    {
    }

    protected function prepareRow(array $row): string
    {
        $names  = "";
        $values = "";
        foreach ($row as $k => $v) {
            $names  .= ",`$k`";
            $values .= ",{$this->connect->escape($v)}";
        }
        return '(' . substr($names, 1) . ') VALUE (' . substr($values, 1) . ')';
    }

    protected function prepareCols(array $cols): string
    {
        $names = "";
        foreach ($cols as $col) {
            $names .= ",`$col`";
        }
        return '(' . substr($names, 1) . ')';
    }

    protected function prepareOnDupKeyUpdate(array $update): string
    {
        $update_statement = "";
        foreach ($update as $k => $v) {
            $update_statement .= ",`$k`={$this->connect->escape($v)}";
        }
        return 'ON DUPLICATE KEY UPDATE ' . substr($update_statement, 1);
    }

    public function insert(
        string $table,
        array  $row,
        bool   $ignore = false,
        string $partition = "",
    ): void
    {
        $this->connect->exec(
            'INSERT ' .
            ($ignore ? ' IGNORE' : '') .
            "`$table`" .
            ($partition ? " PARTITION $partition " : '') .
            "{$this->prepareRow($row)}"
        );
    }

    public function insertSelect(
        string $table,
        array  $cols,
        Select $select,
        bool   $ignore = false,
        string $partition = ""
    ): void
    {
        $this->connect->exec(
            'INSERT ' .
            ($ignore ? ' IGNORE' : '') .
            "`$table`" .
            ($partition ? " PARTITION $partition " : '') .
            "{$this->prepareCols($cols)} {$select->query()}"
        );
    }

    public function insertUpdate(
        string $table,
        array  $row,
        array  $update,
        string $partition = ""
    ): void
    {
        $this->connect->exec(
            "INSERT `$table`" .
            ($partition ? " PARTITION $partition " : '') .
            "{$this->prepareRow($row)} {$this->prepareOnDupKeyUpdate($update)}"
        );
    }

    public function replace(
        string $table,
        array  $row,
        string $partition = ""
    ): void
    {
        $this->connect->exec(
            "REPLACE `$table`" .
            ($partition ? " PARTITION $partition " : '') .
            "{$this->prepareRow($row)}"
        );
    }

    public function replaceSelect(
        string $table,
        array  $cols,
        Select $select,
        string $partition = ""
    ): void
    {
        $this->connect->exec(
            "REPLACE `$table`" .
            ($partition ? " PARTITION $partition " : '') .
            "{$this->prepareCols($cols)} {$select->query()}"
        );
    }

    private static function rowNamesHash($keys): string
    {
        return implode(":", $keys);
    }

    public function insertBulk(
        string $table,
        array  $rows,
        int    $batch = 1000,
        array  $addToRow = [],
        bool   $ignore = false,
        string $partition = "",
        ?array $onDupKeyUpdate = null,
        bool   $deepValidation = true,
    ): int
    {
        $query_begin = 'INSERT';

        if (empty($rows)) {
            throw new UnexpectedValueException('rows must have data');
        }

        reset($rows);
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
            ($partition ? " PARTITION $partition " : '') .
            '(`' . implode("`,`", $allNames) . '`) VALUE ';

        $query_end = is_array($onDupKeyUpdate)
            ? " {$this->prepareOnDupKeyUpdate($onDupKeyUpdate)}"
            : "";

        $num         = 0;
        $blockNum    = 0;
        $affected    = 0;
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
                $row_values .= ",{$this->connect->escape($v)}";
            }
            foreach ($row as $v) {
                $row_values .= ",{$this->connect->escape($v)}";
            }
            $all_values .= ',(' . substr($row_values, 1) . ')';

            if ($blockNum == $batch) {
                $this->connect->exec($query_begin . substr($all_values, 1) . $query_end);
                $affected   += $this->connect->lastAffectedRows();
                $all_values = '';
                $blockNum   = 0;
            }
        }

        if ($blockNum > 0) {
            $this->connect->exec($query_begin . substr($all_values, 1) . $query_end);
            $affected += $this->connect->lastAffectedRows();
        }

        return $affected;
    }


    public function replaceBulk(string $table, array $data, int $batch = 1000, array $base = []): void
    {

    }
}