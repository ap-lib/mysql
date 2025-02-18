<?php declare(strict_types=1);

namespace AP\Mysql\Executable;

use AP\Mysql\Connect\ConnectInterface;
use AP\Mysql\UpsertHelpers;
use Generator;

class InsertBulk implements Executable
{
    public function __construct(
        private readonly ConnectInterface $connect,
        protected string                  $table,
        protected array                   $rows,
        protected int                     $batch = 1000,
        protected array                   $addToRow = [],
        protected bool                    $ignore = false,
        protected string                  $partition = "",
        protected ?array                  $onDupKeyUpdate = null,
        protected bool                    $deepValidation = true,
    )
    {
    }

    public function setTable(string $table): static
    {
        $this->table = $table;
        return $this;
    }

    public function setRows(array $rows): static
    {
        $this->rows = $rows;
        return $this;
    }

    public function setBatch(int $batch): static
    {
        $this->batch = $batch;
        return $this;
    }

    public function setAddToRow(array $addToRow): static
    {
        $this->addToRow = $addToRow;
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

    public function setDeepValidation(bool $deepValidation): static
    {
        $this->deepValidation = $deepValidation;
        return $this;
    }

    /**
     * @return Generator<string>
     */
    public function queries(): Generator
    {
        yield from UpsertHelpers::bulkRunner(
            $this->connect,
            'INSERT',
            is_array($this->onDupKeyUpdate)
                ? ' ' . UpsertHelpers::prepareOnDupKeyUpdate($this->connect, $this->onDupKeyUpdate)
                : '',
            $this->table,
            $this->rows,
            $this->batch,
            $this->addToRow,
            $this->ignore,
            $this->partition,
            $this->deepValidation
        );
    }

    /**
     * @return true
     */
    public function exec(): true
    {
        foreach ($this->queries() as $query) {
            $this->connect->exec($query);
        }
        return true;
    }

    public function execWithAffectedRows(): int
    {
        $affected = 0;
        foreach ($this->queries() as $query) {
            $this->connect->exec($query);
            $affected += $this->connect->lastAffectedRows();
        }
        return $affected;
    }
}