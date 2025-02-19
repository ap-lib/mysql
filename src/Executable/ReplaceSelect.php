<?php declare(strict_types=1);

namespace AP\Mysql\Executable;

use AP\Mysql\Connect\ConnectInterface;
use AP\Mysql\Statement\Statement;
use AP\Mysql\Helpers;

class ReplaceSelect implements Statement, Executable
{
    public function __construct(
        private readonly ConnectInterface $connect,
        protected string                  $table,
        protected Select                  $select,
        protected array                   $cols, // TODO: can be optional if 100% same with select->colsNames
        protected string                  $partition = "",
    )
    {
    }

    public function setTable(string $table): static
    {
        $this->table = $table;
        return $this;
    }

    public function setSelect(Select $select): static
    {
        $this->select = $select;
        return $this;
    }

    public function setCols(array $cols): static
    {
        $this->cols = $cols;
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
            Helpers::prepareCols($this->connect, $this->cols) . ' ' .
            $this->select->query();
    }

    /**
     * @return true
     */
    public function exec(): true
    {
        return $this->connect->exec($this->query());
    }
}