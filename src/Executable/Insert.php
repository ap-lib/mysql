<?php declare(strict_types=1);

namespace AP\Mysql\Executable;

use AP\Mysql\Connect\ConnectInterface;
use AP\Mysql\Statement\Statement;
use AP\Mysql\Helpers;

class Insert implements Statement, Executable
{
    public function __construct(
        private readonly ConnectInterface $connect,
        protected string                  $table,
        protected array                   $row,
        protected bool                    $ignore = false,
        protected ?array                  $onDupKeyUpdate = null,
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

    public function setIgnore(bool $ignore = true): static
    {
        $this->ignore = $ignore;
        return $this;
    }

    public function setPartition(string $partition): static
    {
        $this->partition = $partition;
        return $this;
    }

    public function setOnDupKeyUpdate(?array $onDupKeyUpdate): static
    {
        $this->onDupKeyUpdate = $onDupKeyUpdate;
        return $this;
    }

    public function query(): string
    {
        return 'INSERT ' .
            ($this->ignore ? 'IGNORE ' : '') .
            "`$this->table`" .
            ($this->partition ? " PARTITION ($this->partition)" : '') .
            Helpers::prepareRow($this->connect, $this->row) .
            (!empty($this->onDupKeyUpdate)
                ? ' ' . Helpers::prepareOnDupKeyUpdate($this->connect, $this->onDupKeyUpdate)
                : ''
            );
    }

    /**
     * @return true
     */
    public function exec(): true
    {
        return $this->connect->exec($this->query());
    }
}