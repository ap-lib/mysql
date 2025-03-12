<?php declare(strict_types=1);

namespace AP\Mysql\Executable;

use AP\Mysql\Connect\ConnectInterface;
use AP\Mysql\Helper;
use AP\Mysql\Statement\Statement;

/**
 * Represents an INSERT SQL statement
 *
 * This class provides methods to construct and execute INSERT queries,
 * supporting optional features like IGNORE, partitions, and ON duplicate key update.
 *
 * Important:
 * - Ensure column names are sanitized before passing them to avoid SQL injection
 * - Use `setOnDupKeyUpdate()` only when handling duplicate key scenarios
 *
 * @see https://dev.mysql.com/doc/refman/8.4/en/insert.html
 */
class Insert implements Statement, Executable
{
    /**
     * @param ConnectInterface $connect The database connection instance
     *
     * @param string $table The table name. Don't use raw user input to form the table name
     *                      As it's unsafe for performance reasons
     *                      If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                      If using a scheme name, write it as scheme`.`table to get `scheme`.`table`
     *
     * @param array $row The row data to insert, as an associative array
     *                   The key, column name, isn't safe for performance reasons—don't use raw user input
     *                   If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                   The value, data, will be properly encoded and safe for insertion
     *
     * @param bool $ignore Whether to use IGNORE, preventing errors on duplicate entries
     *
     * @param array|null $onDupKeyUpdate Data for ON duplicate key update.
     *                                   Should be an associative array of column => value
     *                                   The values - data will be properly encoded and safely
     *                                   Don't use raw user input to form the column name
     *                                   If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *
     * @param string $partition The partition to insert into. Don't use raw user input
     */
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

    /**
     * Sets the table for the INSERT statement
     *
     * @param string $table The table name. Don't use raw user input to form the table name
     *                      As it's unsafe for performance reasons
     *                      If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                      If using a scheme name, write it as scheme`.`table to get `scheme`.`table`
     * @return $this
     */
    public function setTable(string $table): static
    {
        $this->table = $table;
        return $this;
    }

    /**
     * Sets the row data to insert
     *
     * @param array $row The row data as an associative array
     *                   The key - column name isn't safe for performance reasons—don't use raw user input
     *                   If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                   The value - data will be properly encoded and safe for insertion
     * @return $this
     */
    public function setRow(array $row): static
    {
        $this->row = $row;
        return $this;
    }

    /**
     * Enables or disables the IGNORE option for the INSERT statement
     *
     * @param bool $ignore Whether to use IGNORE. Defaults to true
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
     * Sets the on duplicate key update values
     *
     * @param array|null $onDupKeyUpdate An associative array of column ⇒ value for updating on duplicate keys
     * @return $this
     */
    public function setOnDupKeyUpdate(?array $onDupKeyUpdate): static
    {
        $this->onDupKeyUpdate = $onDupKeyUpdate;
        return $this;
    }

    /**
     * Builds and returns the final INSERT query string
     *
     * @return string The constructed SQL query
     */
    public function query(): string
    {
        return 'INSERT ' .
            ($this->ignore ? 'IGNORE ' : '') .
            "`$this->table`" .
            ($this->partition ? " PARTITION ($this->partition)" : '') .
            Helper::prepareRow($this->connect, $this->row) .
            (!empty($this->onDupKeyUpdate)
                ? ' ' . Helper::prepareOnDupKeyUpdate($this->connect, $this->onDupKeyUpdate)
                : ''
            );
    }

    /**
     * Executes the INSERT statement
     *
     * @return true Returns true on successful execution
     */
    public function exec(): true
    {
        return $this->connect->exec($this->query());
    }

    /**
     * Executes the INSERT statement and returns the last inserted ID.
     *
     * @return int The ID of the most recently inserted row.
     */
    public function execAndGetLastID(): int
    {
        $this->exec();
        return $this->connect->lastInsertId();
    }
}