<?php declare(strict_types=1);

namespace AP\Mysql\Executable;

use AP\Mysql\Connect\ConnectInterface;
use AP\Mysql\Helper;
use AP\Mysql\Statement\OrderBy;
use AP\Mysql\Statement\Statement;
use AP\Mysql\Statement\Where;
use Closure;

/**
 * Represents a DELETE SQL statement
 *
 * This class provides methods to construct and execute DELETE queries,
 * allowing for conditions, ordering, and limits to be applied dynamically.
 *
 * Important:
 * - Ensure WHERE conditions are properly set to avoid deleting all rows unintentionally
 * - Use ORDER BY and LIMIT for controlled deletions when necessary
 *
 * @see https://dev.mysql.com/doc/refman/8.4/en/delete.html
 */
class Delete implements Statement, Executable
{

    private string $partitions  = "";
    private bool   $ignore      = false;
    private string $table_alias = '';

    /**
     * Initializes a DELETE SQL statement
     *
     * @param ConnectInterface $connect The database connection instance
     *
     * @param string $table The table name. Don't use raw user input to form the table name
     *                      As it's unsafe for performance reasons
     *                      If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                      If using a scheme name, write it as scheme`.`table to get `scheme`.`table`
     *
     * @param Where|array<string|mixed>|null $where
     *      **Recommended: use an associative array*** for better performance
     *       - Example: to delete with "WHERE `id`=123", use ["id" => 123]
     *       - If passing a Where object, ensure it's properly set up before execution
     *       - If Where is set but left unconfigured, it will cause an SQL error
     *       - For performance reasons, this class does not validate Where input, and it expects:
     *         - `null` - no condition
     *         - A non-empty array
     *         - A properly configured Where object
     *
     * @param OrderBy|string|null $order The ordering condition for deletion
     *                                   If using a table alias, write it as alias`.`column to get `alias`.`column`
     *
     * @param int|null $limit The limit for the number of rows to delete
     *                        Use only values 0 and preceding
     *                        For performance reasons, this class doesn't validate the limit
     */
    public function __construct(
        private readonly ConnectInterface $connect,
        private string                    $table,
        private Where|array|null          $where = null,
        private OrderBy|string|null       $order = null,
        private ?int                      $limit = null,
    )
    {
    }

    public function query(): string
    {
        return 'DELETE' .
            ($this->ignore ? ' IGNORE' : '') .
            " FROM `$this->table`" .
            (empty($this->table_alias) ? '' : " AS `$this->table_alias`") .
            (empty($this->partitions) ? '' : " PARTITION ($this->partitions)") .
            Helper::prepareWhere($this->connect, " WHERE", $this->where) .
            (is_string($this->order)
                ? " ORDER BY $this->order"
                : ($this->order instanceof OrderBy ? " ORDER BY {$this->order->query()}" : '')
            ) .
            (is_int($this->limit) ? " LIMIT $this->limit" : '');
    }

    /**
     * @return true
     */
    public function exec(): true
    {
        return $this->connect->exec($this->query());
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Sets the partitions for the DELETE query
     *
     * @param string $partitions The partitions to use. Don't use raw user input to form the partition name
     * @return $this
     */
    public function setPartitions(string $partitions): static
    {
        $this->partitions = $partitions;
        return $this;
    }

    /**
     * Sets the table for the DELETE query
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
     * Sets the table alias for the DELETE query
     *
     * @param string $table_alias The alias for the table. Don't use raw user input to form the table alias
     *                            As it's unsafe for performance reasons
     *                            If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     * @return $this
     */
    public function setTableAlias(string $table_alias): static
    {
        $this->table_alias = $table_alias;
        return $this;
    }

    /**
     * Enables or disables the IGNORE option for the DELETE query
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
     * Sets the LIMIT for the DELETE query
     *
     * @param int|null $limit Use a limit of 0 or greater.
     *                        For performance reasons, this class doesn't validate the limit
     * @return $this
     */
    public function setLimit(?int $limit): static
    {
        $this->limit = $limit;
        return $this;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Retrieves the WHERE object, initializing it if necessary
     *
     * Use this only if you need to modify the WHERE clause dynamically,
     * as it always converts to a Where object, which may impact performance
     *
     * @return Where
     */
    public function getWhereObject(): Where
    {
        if (is_null($this->where)) {
            $this->where = new Where($this->connect);
        } elseif (is_array($this->where)) {
            $where = new Where($this->connect);
            foreach ($this->where as $k => $v) {
                $where->eq($k, $v);
            }
            $this->where = $where;
        }
        return $this->where;
    }

    /**
     * Adds AND a formatted condition to the WHERE clause.
     *
     * @param string $condition The condition string, formatted using sprintf
     * * @param mixed ...$values Values to be inserted into the condition
     * *                         Each value will be properly escaped to ensure MySQL safety
     * @return $this
     */
    public function whereCond(string $condition, mixed ...$values): static
    {
        $this->getWhereObject()->cond($condition, ...$values);
        return $this;
    }

    /**
     * Adds AND an equality condition to the WHERE clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value It'll be safety escaped
     * @return $this
     */
    public function whereEq(string $name, mixed $value): static
    {
        $this->getWhereObject()->eq($name, $value);
        return $this;
    }

    /**
     * Adds AND a NOT EQUAL condition to the WHERE clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function whereNotEq(string $name, mixed $value): static
    {
        $this->getWhereObject()->notEq($name, $value);
        return $this;
    }

    /**
     * Adds AND a GREATER THAN condition to the WHERE clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function whereGt(string $name, mixed $value): static
    {
        $this->getWhereObject()->gt($name, $value);
        return $this;
    }

    /**
     * Adds AND a LESS THAN condition to the WHERE clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function whereLt(string $name, mixed $value): static
    {
        $this->getWhereObject()->lt($name, $value);
        return $this;
    }

    /**
     * Adds AND a GREATER THAN OR EQUAL condition to the WHERE clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function whereGte(string $name, mixed $value): static
    {
        $this->getWhereObject()->gte($name, $value);
        return $this;
    }

    /**
     * Adds AND a LESS THAN OR EQUAL condition to the WHERE clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function whereLte(string $name, mixed $value): static
    {
        $this->getWhereObject()->lte($name, $value);
        return $this;
    }

    /**
     * Adds AND a LIKE condition to the WHERE clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to match. It'll be safely escaped
     * @return $this
     */
    public function whereLike(string $name, mixed $value): static
    {
        $this->getWhereObject()->like($name, $value);
        return $this;
    }

    /**
     * Adds AND a NOT LIKE condition to the WHERE clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to match. It'll be safely escaped
     * @return $this
     */
    public function whereNotLike(string $name, mixed $value): static
    {
        $this->getWhereObject()->notLike($name, $value);
        return $this;
    }

    /**
     * Adds AND an IS NULL condition to the WHERE clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @return $this
     */
    public function whereIsNull(string $name): static
    {
        $this->getWhereObject()->isNull($name);
        return $this;
    }

    /**
     * Adds AND an IS NOT NULL condition to the WHERE clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @return $this
     */
    public function whereIsNotNull(string $name): static
    {
        $this->getWhereObject()->isNotNull($name);
        return $this;
    }

    /**
     * Adds AND a BETWEEN condition to the WHERE clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $start The starting value. It'll be safely escaped
     * @param mixed $end The ending value. It'll be safely escaped
     * @return $this
     */
    public function whereBetween(string $name, mixed $start, mixed $end): static
    {
        $this->getWhereObject()->between($name, $start, $end);
        return $this;
    }

    /**
     * Adds AND an IN condition to the WHERE clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param array|Select $list The list of values or a subquery. It'll be safely escaped
     * @return $this
     */
    public function whereIn(string $name, array|Select $list): static
    {
        $this->getWhereObject()->in($name, $list);
        return $this;
    }

    /**
     * Adds AND a NOT IN condition to the WHERE clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param array|Select $list The list of values or a subquery. It'll be safely escaped
     * @return $this
     */
    public function whereNotIn(string $name, array|Select $list): static
    {
        $this->getWhereObject()->notIn($name, $list);
        return $this;
    }

    /**
     * Adds AND an EXISTS condition to the WHERE clause
     *
     * @param Select $select The subquery to check for existence
     * @return $this
     */
    public function whereExists(Select $select): static
    {
        $this->getWhereObject()->exists($select);
        return $this;
    }

    /**
     * Adds AND a NOT EXISTS condition to the WHERE clause
     *
     * @param Select $select The subquery to check for non-existence
     * @return $this
     */
    public function whereNotExists(Select $select): static
    {
        $this->getWhereObject()->notExists($select);
        return $this;
    }

    /**
     * Adds AND a subquery condition to the WHERE clause
     *
     * @param Where $where The subquery condition
     * @return $this
     */
    public function whereSubWhere(Where $where): static
    {
        $this->getWhereObject()->subWhere($where);
        return $this;
    }

    /**
     * Adds AND a subquery condition using a closure
     * less performs then whereSubWhere
     *
     * @param Closure $sub The closure that generates a Where object
     * @return $this
     */
    public function whereSubFn(Closure $sub): static
    {
        $this->getWhereObject()->subFn($sub);
        return $this;
    }

    /**
     * Adds OR a formatted condition to the WHERE clause.
     *
     * @param string $condition The condition string, formatted using sprintf
     * @param mixed ...$values Values to be inserted into the condition
     *                         Each value will be properly escaped to ensure MySQL safety
     * @return $this
     */
    public function orWhereCond(string $condition, mixed ...$values): static
    {
        $this->getWhereObject()->orCond($condition, ...$values);
        return $this;
    }

    /**
     * Adds OR an equality condition to the WHERE clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value It'll be safety escaped
     * @return $this
     */
    public function orWhereEq(string $name, mixed $value): static
    {
        $this->getWhereObject()->orEq($name, $value);
        return $this;
    }

    /**
     * Adds OR a NOT EQUAL condition to the WHERE clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function orWhereNotEq(string $name, mixed $value): static
    {
        $this->getWhereObject()->orNotEq($name, $value);
        return $this;
    }

    /**
     * Adds OR a GREATER THAN condition to the WHERE clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function orWhereGt(string $name, mixed $value): static
    {
        $this->getWhereObject()->orGt($name, $value);
        return $this;
    }

    /**
     * Adds OR a LESS THAN condition to the WHERE clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function orWhereLt(string $name, mixed $value): static
    {
        $this->getWhereObject()->orLt($name, $value);
        return $this;
    }

    /**
     * Adds OR a GREATER THAN OR EQUAL condition to the WHERE clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function orWhereGte(string $name, mixed $value): static
    {
        $this->getWhereObject()->orGte($name, $value);
        return $this;
    }

    /**
     * Adds OR a LESS THAN OR EQUAL condition to the WHERE clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function orWhereLte(string $name, mixed $value): static
    {
        $this->getWhereObject()->orLte($name, $value);
        return $this;
    }

    /**
     * Adds OR a LIKE condition to the WHERE clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to match. It'll be safely escaped
     * @return $this
     */
    public function orWhereLike(string $name, mixed $value): static
    {
        $this->getWhereObject()->orLike($name, $value);
        return $this;
    }

    /**
     * Adds OR a NOT LIKE condition to the WHERE clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to match. It'll be safely escaped
     * @return $this
     */
    public function orWhereNotLike(string $name, mixed $value): static
    {
        $this->getWhereObject()->orNotLike($name, $value);
        return $this;
    }

    /**
     * Adds OR an IS NULL condition to the WHERE clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @return $this
     */
    public function orWhereIsNull(string $name): static
    {
        $this->getWhereObject()->orIsNull($name);
        return $this;
    }

    /**
     * Adds OR an IS NOT NULL condition to the WHERE clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @return $this
     */
    public function orWhereIsNotNull(string $name): static
    {
        $this->getWhereObject()->orIsNotNull($name);
        return $this;
    }

    /**
     * Adds OR a BETWEEN condition to the WHERE clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $start The starting value. It'll be safely escaped
     * @param mixed $end The ending value. It'll be safely escaped
     * @return $this
     */
    public function orWhereBetween(string $name, mixed $start, mixed $end): static
    {
        $this->getWhereObject()->orBetween($name, $start, $end);
        return $this;
    }

    /**
     * Adds OR an IN condition to the WHERE clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param array|Select $list The list of values or a subquery. It'll be safely escaped
     * @return $this
     */
    public function orWhereIn(string $name, array|Select $list): static
    {
        $this->getWhereObject()->orIn($name, $list);
        return $this;
    }

    /**
     * Adds OR a NOT IN condition to the WHERE clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param array|Select $list The list of values or a subquery. It'll be safely escaped
     * @return $this
     */
    public function orWhereNotIn(string $name, array|Select $list): static
    {
        $this->getWhereObject()->orNotIn($name, $list);
        return $this;
    }

    /**
     * Adds OR an EXISTS condition to the WHERE clause
     *
     * @param Select $select The subquery to check for existence
     * @return $this
     */
    public function orWhereExists(Select $select): static
    {
        $this->getWhereObject()->orExists($select);
        return $this;
    }

    /**
     * Adds OR a NOT EXISTS condition to the WHERE clause
     *
     * @param Select $select The subquery to check for non-existence
     * @return $this
     */
    public function orWhereNotExists(Select $select): static
    {
        $this->getWhereObject()->orNotExists($select);
        return $this;
    }

    /**
     * Adds OR a subquery condition to the WHERE clause
     *
     * @param Where $where The subquery condition
     * @return $this
     */
    public function orWhereSubWhere(Where $where): static
    {
        $this->getWhereObject()->orSubWhere($where);
        return $this;
    }

    /**
     * Adds OR a subquery condition using a closure
     * less performs then whereSubWhere
     *
     * @param Closure $sub The closure that generates a Where object
     * @return $this
     */
    public function orWhereSubFn(Closure $sub): static
    {
        $this->getWhereObject()->orSubFn($sub);
        return $this;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Retrieves the OrderBy object, initializing it if necessary
     *
     * Use this only if you need to modify the "ORDER BY" clause dynamically,
     * as it always converts to an OrderBy object, which may impact performance
     *
     * @return OrderBy
     */
    public function getOrderObject(): OrderBy
    {
        if (is_null($this->order)) {
            $this->order = new OrderBy();
        } elseif (is_string($this->order)) {
            $this->order = (new OrderBy)->expr($this->order);
        }
        return $this->order;
    }

    /**
     * Adds an ascending order condition to the ORDER BY clause
     *
     * @param string|int $name The column name or index to order by. The name will be just wrapped in backticks (`)
     *                         Don't use raw user input directly to form column names.
     *                         If ordering by an aliased column, write it as o`.`column to get `o`.`column`
     * @return $this
     */
    public function order(string|int $name): static
    {
        $this->getOrderObject()->asc($name);
        return $this;
    }

    /**
     * Adds a descending order condition to the ORDER BY clause
     *
     * @param string|int $name The column name or index to order by. The name will be just wrapped in backticks (`)
     *                         Don't use raw user input directly to form column names.
     *                         If ordering by an aliased column, write it as o`.`column to get `o`.`column`
     * @return $this
     */
    public function orderDesc(string|int $name): static
    {
        $this->getOrderObject()->desc($name);
        return $this;
    }

    /**
     * Adds an expression-based ascending order condition to the ORDER BY clause
     *
     * @param string $expr The expression used for ordering
     * @return $this
     */
    public function orderExpr(string $expr): static
    {
        $this->getOrderObject()->expr($expr);
        return $this;
    }

    /**
     * Adds an expression-based descending order condition to the ORDER BY clause
     *
     * @param string $expr The expression used for ordering
     * @return $this
     */
    public function orderExprDesc(string $expr): static
    {
        $this->getOrderObject()->exprDesc($expr);
        return $this;
    }
}