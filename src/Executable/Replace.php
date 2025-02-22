<?php declare(strict_types=1);

namespace AP\Mysql\Executable;

use AP\Mysql\Connect\ConnectInterface;
use AP\Mysql\Statement\Statement;
use AP\Mysql\Helper;

/**
 * Represents a REPLACE SQL statement
 *
 * This class provides methods to construct and execute REPLACE queries,
 * allowing for the replacement of existing rows if a duplicate key is found.
 * It supports optional partitioning.
 *
 * Important:
 * - Ensure column names are sanitized before passing them to avoid SQL injection
 * - REPLACE works similarly to INSERT but will delete existing rows with the same key before inserting new ones
 *
 * @see https://dev.mysql.com/doc/refman/8.4/en/replace.html
 */
class Replace implements Statement, Executable
{
    /**
     * @param ConnectInterface $connect The database connection instance
     *
     * @param string $table The table name. Don't use raw user input to form the table name
     *                      As it's unsafe for performance reasons
     *                      If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                      If using a schema name, write it as schema`.`table to get `schema`.`table`
     *
     * @param array $row The row data to insert, as an associative array
     *                   The key (column name) isn't safe for performance reasons—don't use raw user input
     *                   If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                   The value (data) will be properly encoded and safe for insertion
     *
     * @param string $partition The partition to insert into. Don't use raw user input
     */
    public function __construct(
        private readonly ConnectInterface $connect,
        protected string                  $table,
        protected array                   $row,
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
     *                      If using a schema name, write it as schema`.`table to get `schema`.`table`
     * @return $this
     */
    public function setTable(string $table): static
    {
        $this->table = $table;
        return $this;
    }

    /**
     * Sets the row data to replace
     *
     * @param array $row The row data as an associative array
     *                   The key (column name) isn't safe for performance reasons—don't use raw user input
     *                   If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                   The value (data) will be properly encoded and safe for insertion
     * @return $this
     */
    public function setRow(array $row): static
    {
        $this->row = $row;
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
     * Builds and returns the final REPLACE query string
     *
     * @return string The constructed SQL query
     */
    public function query(): string
    {
        return 'REPLACE ' .
            "`$this->table`" .
            ($this->partition ? " PARTITION ($this->partition)" : '') .
            Helper::prepareRow($this->connect, $this->row);
    }

    /**
     * Executes the REPLACE statement
     *
     * @return true Returns true on successful execution
     */
    public function exec(): true
    {
        return $this->connect->exec($this->query());
    }
}