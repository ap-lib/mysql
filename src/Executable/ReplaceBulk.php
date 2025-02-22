<?php declare(strict_types=1);

namespace AP\Mysql\Executable;

use AP\Mysql\Connect\ConnectInterface;
use AP\Mysql\Helper;
use Generator;

/**
 * Represents a bulk REPLACE SQL generator of statements
 *
 * This class allows inserting multiple rows in batches using the REPLACE statement, with support for:
 * - Partitioned inserts
 * - Deep validation for data integrity
 *
 * Important:
 * - Column names (keys in $rows) are not safe for performance reasons—don't use raw user input
 *   If needed, use AP\Mysql\Helpers::escapeName() to sanitize them
 * - Values (data) will be properly encoded and safe for MySQL insertion
 * - The batch size determines how many rows are inserted per query
 * - REPLACE works similarly to INSERT but will delete existing rows with the same key before inserting new ones
 *
 * @see https://dev.mysql.com/doc/refman/8.4/en/replace.html
 */
class ReplaceBulk implements Executable
{
    /**
     * @param ConnectInterface $connect The database connection instance
     * @param string $table The table name. Don't use raw user input to form the table name
     *                      As it's unsafe for performance reasons
     *                      If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *
     * @param array<array<string, mixed>> $rows The rows to insert, each as an associative array
     *                                          The key (column name) is not safe—don't use raw user input
     *                                          If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                                          The values (data) will be properly encoded and safe
     *
     * @param int $batch The batch size for bulk inserts
     *
     * @param array<string, mixed> $addToRow Additional key-value pairs to add to each row before insertion
     *                                       The key (column name) is not safe—don't use raw user input
     *                                       If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                                       The values (data) will be properly encoded and safe
     *
     * @param string $partition The partition to insert into. Don't use raw user input
     *
     * @param bool $deepValidation Whether to perform deep validation on the data
     *                             false can boost performance
     */
    public function __construct(
        private readonly ConnectInterface $connect,
        protected string                  $table,
        protected array                   $rows,
        protected int                     $batch = 1000,
        protected array                   $addToRow = [],
        protected string                  $partition = "",
        protected bool                    $deepValidation = true,
    )
    {
    }

    /**
     * Sets the table for the REPLACE statement
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
     * Sets the rows for bulk replace
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
     * Sets the batch size for bulk replace
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
     * Sets additional key-value pairs to be added to each row before replace
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
     * Sets the partition for the REPLACE statement
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
     * Generates and yields REPLACE queries for bulk execution
     *
     * @return Generator<string> A generator yielding REPLACE query strings
     */
    public function queries(): Generator
    {
        yield from Helper::bulkRunner(
            $this->connect,
            'REPLACE',
            '',
            $this->table,
            $this->rows,
            $this->batch,
            $this->addToRow,
            false,
            $this->partition,
            $this->deepValidation
        );
    }

    /**
     * Executes all generated REPLACE queries
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
     * Executes all generated REPLACE queries and returns the total affected rows
     *
     * @return int The total number of rows affected by the REPLACE statements
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