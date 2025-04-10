<?php declare(strict_types=1);

namespace AP\Mysql\Executable;

use AP\Mysql\Connect\ConnectInterface;
use AP\Mysql\Helper;
use Generator;

/**
 * Represents a bulk INSERT SQL generator os statements
 *
 * This class allows inserting multiple rows in batches, with support for:
 * - IGNORE option to prevent errors on duplicate entries
 * - Partitioned inserts
 * - ON DUPLICATE KEY UPDATE handling
 *
 * Important:
 * - Column names (keys in $rows) are not safe for performance reasons—don't use raw user input
 *   If needed, use AP\Mysql\Helpers::escapeName() to sanitize them
 * - Values (data) will be properly encoded and safe for MySQL insertion
 * - The batch size determines how many rows are inserted per query
 *
 * @see https://dev.mysql.com/doc/refman/8.4/en/insert.html
 */
class InsertBulk implements Executable
{
    /**
     * @param ConnectInterface $connect The database connection instance
     * @param string $table The table name. Don't use raw user input to form the table name
     *                      As it's unsafe for performance reasons
     *                      If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     * @param array<array<string, mixed>> $rows The rows to insert, each as an associative array
     *                                          The key (column name) is not safe—don't use raw user input
     *                                          If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                                          The values (data) will be properly encoded and safe
     * @param int $batch The batch size for bulk inserts
     * @param array<string, mixed> $addToRow Additional key-value pairs to add to each row before insertion
     *                                       The key (column name) is not safe—don't use raw user input
     *                                       If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                                       The values (data) will be properly encoded and safe
     * @param bool $ignore Whether to use IGNORE, preventing errors on duplicate entries
     * @param string $partition The partition to insert into. Don't use raw user input
     * @param array<string, mixed>|null $onDupKeyUpdate Data for ON DUPLICATE KEY UPDATE
     *                                                  Should be an associative array of column => value
     *                                                  The values (data) will be properly encoded and safe
     *                                                  Don't use raw user input to form the column name
     *                                                  If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     * @param bool $deepValidation Whether to perform deep validation on the data
     *                             false can boost performance
     */
    public function __construct(
        public readonly ConnectInterface $connect,
        protected string                 $table,
        protected array                  $rows,
        protected int                    $batch = 1000,
        protected array                  $addToRow = [],
        protected bool                   $ignore = false,
        protected string                 $partition = "",
        protected ?array                 $onDupKeyUpdate = null,
        protected bool                   $deepValidation = true,
    )
    {
    }

    /**
     * Sets the table for the INSERT statement
     *
     * @param string $table The table name. Don't use raw user input to form the table name
     *                      As it's unsafe for performance reasons
     *                      If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     * @return $this
     */
    public function setTable(string $table): static
    {
        $this->table = $table;
        return $this;
    }

    /**
     * Sets the rows for bulk insertion
     *
     * @param array<array<string, mixed>> $rows The rows to insert, each as an associative array
     *                                          The key (column name) is not safe—don't use raw user input
     *                                          If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                                          The values (data) will be properly encoded and safe
     * @return $this
     */
    public function setRows(array $rows): static
    {
        $this->rows = $rows;
        return $this;
    }

    /**
     * Sets the batch size for bulk insertion
     *
     * @param int $batch The number of rows to insert per query
     * @return $this
     */
    public function setBatch(int $batch): static
    {
        $this->batch = $batch;
        return $this;
    }

    /**
     * Sets additional key-value pairs to be added to each row before insertion
     *
     * @param array<string, mixed> $addToRow Additional column values to include in every inserted row
     *                                       The key (column name) is not safe—don't use raw user input
     *                                       If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                                       The values (data) will be properly encoded and safe
     * @return $this
     */
    public function setAddToRow(array $addToRow): static
    {
        $this->addToRow = $addToRow;
        return $this;
    }

    /**
     * Enables or disables the IGNORE option for the INSERT statement
     *
     * @param bool $ignore Whether to use IGNORE
     * @return $this
     */
    public function setIgnore(bool $ignore = true): static
    {
        $this->ignore = $ignore;
        return $this;
    }

    /**
     * Sets the partition for the INSERT statement
     *
     * @param string $partition The partition name. Don't use raw user input
     * @return $this
     */
    public function setPartition(string $partition): static
    {
        $this->partition = $partition;
        return $this;
    }

    /**
     * Sets the ON DUPLICATE KEY UPDATE values
     *
     * @param array|null $onDupKeyUpdate An associative array of column => value for updating on duplicate keys
     *                                   Should be an associative array of column => value
     *                                   The values (data) will be properly encoded and safe
     *                                   Don't use raw user input to form the column name
     *                                   If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     * @return $this
     */
    public function setOnDupKeyUpdate(?array $onDupKeyUpdate): static
    {
        $this->onDupKeyUpdate = $onDupKeyUpdate;
        return $this;
    }

    /**
     * Enables or disables deep validation on the data
     *
     * @param bool $deepValidation Whether to perform deep validation
     * @return $this
     */
    public function setDeepValidation(bool $deepValidation): static
    {
        $this->deepValidation = $deepValidation;
        return $this;
    }

    /**
     * Generates and yields INSERT queries for bulk execution
     *
     * @return Generator<string>
     */
    public function queries(): Generator
    {
        yield from Helper::bulkRunner(
            $this->connect,
            'INSERT',
            is_array($this->onDupKeyUpdate)
                ? ' ' . Helper::prepareOnDupKeyUpdate($this->connect, $this->onDupKeyUpdate)
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
     * Executes all generated INSERT queries
     *
     * @return true Returns true on successful execution
     */
    public function exec(): true
    {
        foreach ($this->queries() as $query) {
            $this->connect->exec($query);
        }
        return true;
    }

    /**
     * Executes all generated INSERT queries and returns the total affected rows
     *
     * @return int The total number of rows affected by the INSERT statements
     */
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