<?php declare(strict_types=1);

namespace AP\Mysql\Executable;

use AP\Mysql\Connect\ConnectInterface;
use AP\Mysql\Statement\Statement;
use AP\Mysql\Helper;

/**
 * Represents an INSERT ... SELECT SQL statement
 *
 * This class allows inserting data into a table by selecting it from another table,
 * supporting optional features such as:
 * - IGNORE option to prevent errors on duplicate entries
 * - Partitioned inserts
 * - ON DUPLICATE KEY UPDATE handling
 *
 * Important:
 * - Column names (keys in $cols) are not safe for performance reasons—don't use raw user input
 *   If needed, use AP\Mysql\Helpers::escapeName() to sanitize them
 * - The SELECT statement should be properly constructed to match the column structure
 *
 * @see https://dev.mysql.com/doc/refman/8.4/en/insert-select.html
 */
class InsertSelect implements Statement, Executable
{
    /**
     * @param ConnectInterface $connect The database connection instance
     *
     * @param string $table The table name. Don't use raw user input to form the table name
     *                      As it's unsafe for performance reasons
     *                      If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *
     * @param Select $select The SELECT query that provides data for insertion
     *
     * @param array<string> $cols The list of column names for insertion.
     *                            The column names must match the SELECT statement's output columns
     *                            The key (column name) is not safe—don't use raw user input
     *                            If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *
     * @param bool $ignore Whether to use IGNORE, preventing errors on duplicate entries
     *
     * @param array|null $onDupKeyUpdate Data for ON DUPLICATE KEY UPDATE.
     *                                   Should be an associative array of column => value
     *                                   The values (data) will be properly encoded and safe
     *                                   Don't use raw user input to form the column name
     *                                   If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *
     * @param string $partition The partition to insert into. Don't use raw user input
     */
    public function __construct(
        private readonly ConnectInterface $connect,
        protected string                  $table,
        protected Select                  $select,
        protected array                   $cols = [],
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
     * @return $this
     */
    public function setTable(string $table): static
    {
        $this->table = $table;
        return $this;
    }

    /**
     * Sets the SELECT query that provides data for insertion
     *
     * @param Select $select The SELECT statement
     * @return $this
     */
    public function setSelect(Select $select): static
    {
        $this->select = $select;
        return $this;
    }

    /**
     * Sets the column names for insertion
     *
     * @param array<string> $cols The list of column names. The names must match the SELECT statement's output columns
     *                            The key (column name) is not safe—don't use raw user input
     *                            If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     * @return $this
     */
    public function setCols(array $cols): static
    {
        $this->cols = $cols;
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
     * @return $this
     */
    public function setOnDupKeyUpdate(?array $onDupKeyUpdate): static
    {
        $this->onDupKeyUpdate = $onDupKeyUpdate;
        return $this;
    }

    /**
     * Builds and returns the final INSERT SELECT query string
     *
     * @return string The constructed SQL query
     */
    public function query(): string
    {
        return 'INSERT ' .
            ($this->ignore ? 'IGNORE ' : '') .
            "`$this->table`" .
            ($this->partition ? " PARTITION ($this->partition)" : '') .
            Helper::prepareCols($this->cols, $this->select) . ' ' .
            $this->select->query() .
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
}