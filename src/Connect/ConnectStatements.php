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
use AP\Mysql\Executable\Update;
use AP\Mysql\Statement\TableFactor;
use AP\Mysql\Statement\Where;

trait ConnectStatements
{
    public function delete(
        string $table
    ): Delete
    {
        return new Delete(
            $this,
            $table
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
        array  $cols = []
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
    ): Select
    {
        return new Select(
            $this,
            $table,
            $columns,
        );
    }

    public function update(string $table): Update
    {
        return new Update(
            $this,
            $table,
        );
    }

    public function where(): Where
    {
        return new Where($this);
    }
}