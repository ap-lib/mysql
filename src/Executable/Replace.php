<?php declare(strict_types=1);

namespace AP\Mysql\Executable;

use AP\Mysql\Connect\ConnectInterface;
use AP\Mysql\Statement\Statement;
use AP\Mysql\UpsertHelpers;

class Replace implements Statement, Executable
{
    public function __construct(
        private readonly ConnectInterface $connect,
        protected string                  $table,
        protected array                   $row,
        protected string                  $partition = "",
    )
    {
    }

    public function setTable(string $table): static
    {
        $this->table = $table;
        return $this;
    }

    public function setRow(array $row): static
    {
        $this->row = $row;
        return $this;
    }

    public function setPartition(string $partition): static
    {
        $this->partition = $partition;
        return $this;
    }

    public function query(): string
    {
        return 'REPLACE ' .
            "`$this->table`" .
            ($this->partition ? " PARTITION ($this->partition)" : '') .
            UpsertHelpers::prepareRow($this->connect, $this->row);
    }

    /**
     * @return true
     */
    public function exec(): true
    {
        return $this->connect->exec($this->query());
    }
}