<?php declare(strict_types=1);

namespace AP\Mysql\Statement;

use AP\Mysql\Executable\Select;

readonly class TableFactor implements Statement
{
    /**
     * @param string|Select $table
     * @param string $alias
     * @param string $partition
     * @param string $indexHintList https://dev.mysql.com/doc/refman/8.4/en/index-hints.html
     */
    public function __construct(
        private string|Select $table,
        private string        $alias = "",
        private string        $partition = "",
        private string        $indexHintList = "",

    )
    {
    }

    public function query(): string
    {
        return is_string($this->table)
            ? (
                "`$this->table`" .
                ($this->partition ? " PARTITION ($this->partition)" : '') .
                (empty($this->alias) ? '' : " AS `$this->alias`") .
                (empty($this->indexHintList) ? '' : " $this->indexHintList")
            )
            : (
                "({$this->table->query()})" .
                (empty($this->alias) ? '' : " AS `$this->alias`")
            );

    }
}