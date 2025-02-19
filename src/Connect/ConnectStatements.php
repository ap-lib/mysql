<?php declare(strict_types=1);

namespace AP\Mysql\Connect;

use AP\Mysql\Executable\Delete;
use AP\Mysql\Executable\Insert;
use AP\Mysql\Executable\InsertBulk;
use AP\Mysql\Executable\InsertSelect;
use AP\Mysql\Executable\Replace;
use AP\Mysql\Executable\ReplaceBulk;
use AP\Mysql\Executable\ReplaceSelect;
use AP\Mysql\Executable\Select;
use AP\Mysql\Statement\GroupBy;
use AP\Mysql\Statement\OrderBy;
use AP\Mysql\Statement\TableFactor;
use AP\Mysql\Statement\Where;

trait ConnectStatements
{
    public function delete(
        string $table,
        ?array $where = null,
        ?int   $limit = null
    ): Delete
    {
        return new Delete(
            $this,
            $table,
            $where,
            $limit
        );
    }

    public function insert(
        string $table,
        array  $row
    ): Insert
    {
        return new Insert(
            $this,
            $table,
            $row
        );
    }

    public function insertSelect(
        string $table,
        Select $select,
        array  $cols
    ): InsertSelect
    {
        return new InsertSelect(
            $this,
            $table,
            $select,
            $cols
        );
    }

    public function insertBulk(
        string $table,
        array  $rows
    ): InsertBulk
    {
        return new InsertBulk(
            $this,
            $table,
            $rows
        );
    }

    public function replace(
        string $table,
        array  $row
    ): Replace
    {
        return new Replace(
            $this,
            $table,
            $row
        );
    }

    public function replaceSelect(
        string $table,
        Select $select,
        array  $cols
    ): ReplaceSelect
    {
        return new ReplaceSelect(
            $this,
            $table,
            $select,
            $cols
        );
    }

    public function replaceBulk(
        string $table,
        array  $rows
    ): ReplaceBulk
    {
        return new ReplaceBulk(
            $this,
            $table,
            $rows
        );
    }

    ////////////////

    public function select(
        string|TableFactor $table,
        array              $columns = [],
        Where|array|null   $where = null,
        Where|array|null   $having = null,
        ?GroupBy           $group = null,
        ?OrderBy           $order = null,
        ?int               $limit = null,
        ?int               $offset = null,
        bool               $distinct = false,
        bool               $straightJoin = false,
        ?bool              $sqlSmallResult = null,
    )
    {
        return new Select(
            $this,
            $table,
            $columns,
            $where,
            $having,
            $group,
            $order,
            $limit,
            $offset,
            $distinct,
            $straightJoin,
            $sqlSmallResult,
        );
    }

    public function update(
        string $table,
        array  $assignment,
               $where_condition,
               $order_by = null,
        bool   $ignore = false,
    )
    {

    }


    public function call(
        string $name,
        array  $parameters,
    )
    {

    }
}