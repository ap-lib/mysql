<?php declare(strict_types=1);

namespace AP\Mysql\Executable;

use AP\Mysql\Connect\ConnectInterface;
use AP\Mysql\Helper;
use AP\Mysql\Statement\Statement;

/**
 * Represents a REPLACE ... SELECT SQL statement
 *
 * This class allows replacing data in a table by selecting it from another table,
 * supporting optional features such as:
 * - Partitioned inserts
 *
 * Important:
 * - Column names (keys in $cols) are not safe for performance reasons—don't use raw user input
 *   If needed, use AP\Mysql\Helpers::escapeName() to sanitize them
 * - The SELECT statement should be properly constructed to match the column structure
 * - REPLACE works similarly to INSERT but will delete existing rows with the same key before inserting new ones
 *
 * @see https://dev.mysql.com/doc/refman/8.4/en/replace.html
 */
class ReplaceSelect implements Statement, Executable
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
     * @param string $partition The partition to insert into. Don't use raw user input
     */
    public function __construct(
        private readonly ConnectInterface $connect,
        protected string                  $table,
        protected Select                  $select,
        protected array                   $cols = [],
        protected string                  $partition = "",
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
     * Sets the SELECT query that provides data for replacement
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
     * Sets the column names for replacement
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
     * Builds and returns the final REPLACE ... SELECT query string
     *
     * @return string The constructed SQL query
     */
    public function query(): string
    {
        return 'REPLACE ' .
            "`$this->table`" .
            ($this->partition ? " PARTITION ($this->partition)" : '') .
            Helper::prepareCols($this->cols) . ' ' .
            $this->select->query();
    }

    /**
     * Executes the REPLACE ... SELECT statement
     *
     * @return true Returns true on successful execution
     */
    public function exec(): true
    {
        return $this->connect->exec($this->query());
    }
}