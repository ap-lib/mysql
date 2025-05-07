<?php declare(strict_types=1);

namespace AP\Mysql\Executable;

use AP\Mysql\Connect\ConnectInterface;
use AP\Mysql\Helper;
use AP\Mysql\Statement\Statement;
use AP\Mysql\Statement\Where;
use Closure;

/**
 * Represents UPDATE SQL statement
 *
 * This class provides methods to construct and execute DELETE queries,
 * allowing for conditions, ordering, and limits to be applied dynamically.
 *
 * Important:
 * - Ensure where conditions are properly set to avoid deleting all rows unintentionally
 * - Use order by and limit for controlled deletions when necessary
 *
 * @see https://dev.mysql.com/doc/refman/8.4/en/delete.html
 */
class Update implements Statement, Executable
{

    private string $partitions  = "";
    private bool   $ignore      = false;
    private string $table_alias = '';
    private string $where       = "";
    private string $assignments = "";
    private string $order       = "";
    private ?int   $limit       = null;

    /**
     * Initializes a DELETE SQL statement
     *
     * @param ConnectInterface $connect The database connection instance
     *
     * @param string $table The table name. Don't use raw user input to form the table name
     *                      As it's unsafe for performance reasons
     *                      If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     *                      If using a scheme name, write it as scheme`.`table to get `scheme`.`table`
     */
    public function __construct(
        public readonly ConnectInterface $connect,
        private string                    $table,

    )
    {
    }

    public function query(): string
    {
        return 'UPDATE' .
            ($this->ignore ? ' IGNORE' : '') .
            " `$this->table`" .
            (empty($this->table_alias) ? '' : " AS `$this->table_alias`") .
            (empty($this->partitions) ? '' : " PARTITION ($this->partitions)") .
            ' SET ' . substr($this->assignments, 1) .
            (empty($this->where) ? '' : ' WHERE ' . substr($this->where, 5)) .
            (empty($this->order) ? '' : ' ORDER BY ' . substr($this->order, 1)) .
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


    public function assignment(string|array $name, mixed $value): static
    {
        $this->assignments .= "," . Helper::escapeNameUnsafe($name) . "={$this->connect->escape($value)}";
        return $this;
    }

    /**
     * @param array<string,mixed> $assignments Data for "set assignment_list"
     *                                         Should be an associative array of column => value
     *                                         The values - data will be properly encoded and safely
     *                                         Don't use raw user input to form the column name
     *                                         If needed, use AP\Mysql\Helpers::escapeName() to sanitize it
     * @return $this
     */
    public function assignments(array $assignments): static
    {
        foreach ($assignments as $name => $value) {
            $this->assignments .= "," . Helper::escapeNameUnsafe($name) . "={$this->connect->escape($value)}";
        }
        return $this;
    }

    /**
     * Sets the partitions for the UPDATE query
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
     * Sets the table for the UPDATE query
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
     * Sets the table alias for the UPDATE query
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
     * Enables or disables the IGNORE option for the UPDATE query
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
     * Sets the limit for the UPDATE query
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
     * Adds an ascending order condition to the "order by" clause
     *
     * @param string|array|int $name The column name or index to order by. The name will be just wrapped in backticks
     *                         Don't use raw user input directly to form column names.
     *                         If ordered by an aliased column, write it as o`.`column to get `o`.`column`
     *                         or use array ['o', 'column']
     * @return $this
     */
    public function order(string|array|int $name): static
    {
        $this->order .= is_int($name) ?
            ",$name"
            : "," . Helper::escapeNameUnsafe($name);

        return $this;
    }

    /**
     * Adds a descending order condition to the "order by" clause
     *
     * @param string|array|int $name The column name or index to order by. The name will be just wrapped in backticks
     *                         Don't use raw user input directly to form column names.
     *                         If ordered by an aliased column, write it as o`.`column to get `o`.`column`
     *                         or use array ['o', 'column']
     * @return $this
     */
    public function orderDesc(string|array|int $name): static
    {
        $this->order .= is_int($name)
            ? ",$name DESC"
            : "," . Helper::escapeNameUnsafe($name) . " DESC";
        return $this;
    }

    /**
     * Adds an expression-based ascending order condition to the "order by" clause
     *
     * @param string $expr The expression used for ordering
     * @return $this
     */
    public function orderExpr(string $expr): static
    {
        $this->order .= ",$expr";
        return $this;
    }

    /**
     * Adds an expression-based descending order condition to the "order by" clause
     *
     * @param string $expr The expression used for ordering
     * @return $this
     */
    public function orderExprDesc(string $expr): static
    {
        $this->order .= ",$expr DESC";
        return $this;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Adds "And" a formatted condition to the "where" clause.
     *
     * @param string $condition The condition string, formatted using sprintf
     * * @param mixed ...$values Values to be inserted into the condition
     * *                         Each value will be properly escaped to ensure MySQL safety
     * @return $this
     */
    public function whereCond(string $condition, mixed ...$values): static
    {
        if (!empty($values)) {
            foreach ($values as $k => &$v) {
                $values[$k] = $this->connect->escape($v);
            }
            $this->where .= ' AND (' . sprintf($condition, ...$values) . ')';
        } else {
            $this->where .= " AND ($condition)";
        }
        return $this;
    }

    /**
     * Adds "And" an equality condition to the "where" clause
     *
     * @param string|array $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value It'll be safety escaped
     * @return $this
     */
    public function whereEq(string|array $name, mixed $value): static
    {
        $this->where .= " AND " . Helper::escapeNameUnsafe($name) . "={$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds "And" a Not Equal condition to the "where" clause
     *
     * @param string|array $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function whereNotEq(string|array $name, mixed $value): static
    {
        $this->where .= " AND " . Helper::escapeNameUnsafe($name) . "<>{$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds "And" a Greater than condition to the "where" clause
     *
     * @param string|array $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function whereGt(string|array $name, mixed $value): static
    {
        $this->where .= " AND " . Helper::escapeNameUnsafe($name) . ">{$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds "And" a Less than condition to the "where" clause
     *
     * @param string|array $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function whereLt(string|array $name, mixed $value): static
    {
        $this->where .= " AND " . Helper::escapeNameUnsafe($name) . "<{$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds "And" a Greater than OR Equal condition to the "where" clause
     *
     * @param string|array $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function whereGte(string|array $name, mixed $value): static
    {
        $this->where .= " AND " . Helper::escapeNameUnsafe($name) . ">={$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds "And" a Less than OR Equal condition to the "where" clause
     *
     * @param string|array $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function whereLte(string|array $name, mixed $value): static
    {
        $this->where .= " AND " . Helper::escapeNameUnsafe($name) . "<={$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds "And" a like condition to the "where" clause
     *
     * @param string|array $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to match. It'll be safely escaped
     * @return $this
     */
    public function whereLike(string|array $name, mixed $value): static
    {
        $this->where .= " AND " . Helper::escapeNameUnsafe($name) . " LIKE {$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds "And" a not like condition to the "where" clause
     *
     * @param string|array $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to match. It'll be safely escaped
     * @return $this
     */
    public function whereNotLike(string|array $name, mixed $value): static
    {
        $this->where .= " AND " . Helper::escapeNameUnsafe($name) . " NOT LIKE {$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds "And" Is Null condition to the "where" clause
     *
     * @param string|array $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @return $this
     */
    public function whereIsNull(string|array $name): static
    {
        $this->where .= " AND " . Helper::escapeNameUnsafe($name) . " IS NULL";
        return $this;
    }

    /**
     * Adds "And" Isn't Null condition to the "where" clause
     *
     * @param string|array $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @return $this
     */
    public function whereIsNotNull(string|array $name): static
    {
        $this->where .= " AND " . Helper::escapeNameUnsafe($name) . " IS NOT NULL";
        return $this;
    }

    /**
     * Adds "And" a BETWEEN condition to the "where" clause
     *
     * @param string|array $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $start The starting value. It'll be safely escaped
     * @param mixed $end The ending value. It'll be safely escaped
     * @return $this
     */
    public function whereBetween(string|array $name, mixed $start, mixed $end): static
    {
        $this->where .= " AND " . Helper::escapeNameUnsafe($name) . " BETWEEN {$this->connect->escape($start)} AND {$this->connect->escape($end)}";
        return $this;
    }

    /**
     * Adds "And" an IN condition to the "where" clause
     *
     * @param string|array $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param array|Select $list The list of values or a sub query. It'll be safely escaped
     * @return $this
     */
    public function whereIn(string|array $name, array|Select $list): static
    {
        if (is_array($list)) {
            if (empty($list)) {
                $this->where .= " AND FALSE";
                return $this;
            }
            foreach ($list as $k => $v) {
                $list[$k] = $this->connect->escape($v);
            }
            $list = '(' . implode(',', $list) . ')';
        } else {
            $list = "({$list->query()})";
        }

        $this->where .= " AND " . Helper::escapeNameUnsafe($name) . " IN $list";
        return $this;
    }

    /**
     * Adds "And" a not IN condition to the "where" clause
     *
     * @param string|array $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param array|Select $list The list of values or a sub query. It'll be safely escaped
     * @return $this
     */
    public function whereNotIn(string|array $name, array|Select $list): static
    {
        if (is_array($list)) {
            if (empty($list)) {
                return $this;
            }
            foreach ($list as $k => $v) {
                $list[$k] = $this->connect->escape($v);
            }
            $list = '(' . implode(',', $list) . ')';
        } else {
            $list = "({$list->query()})";
        }

        $this->where .= " AND " . Helper::escapeNameUnsafe($name) . " NOT IN $list";
        return $this;
    }

    /**
     * Adds "And" an EXISTS condition to the "where" clause
     *
     * @param Select $select The sub query to check for existence
     * @return $this
     */
    public function whereExists(Select $select): static
    {
        $this->where .= " AND EXISTS ({$select->query()})";
        return $this;
    }

    /**
     * Adds "And" a not EXISTS condition to the "where" clause
     *
     * @param Select $select The sub query to check for non-existence
     * @return $this
     */
    public function whereNotExists(Select $select): static
    {
        $this->where .= " AND NOT EXISTS ({$select->query()})";
        return $this;
    }

    /**
     * Adds "And" a sub query condition to the "where" clause
     *
     * @param Where $where The sub query condition
     * @return $this
     */
    public function whereSubWhere(Where $where): static
    {
        $this->where .= " AND ({$where->query()})";
        return $this;
    }

    /**
     * Adds "And" a sub query condition using a closure
     * less performs then whereSubWhere
     *
     * @param Closure $sub The closure that generates a Where object
     * @return $this
     */
    public function whereSubFn(Closure $sub): static
    {
        $this->where .= ' AND (' . $sub(new Where($this->connect))->query() . ')';
        return $this;
    }

    /**
     * Adds OR a formatted condition to the "where" clause.
     *
     * @param string $condition The condition string, formatted using sprintf
     * @param mixed ...$values Values to be inserted into the condition
     *                         Each value will be properly escaped to ensure MySQL safety
     * @return $this
     */
    public function orWhereCond(string $condition, mixed ...$values): static
    {
        if (!empty($values)) {
            foreach ($values as $k => &$v) {
                $values[$k] = $this->connect->escape($v);
            }
            $this->where .= ' OR  (' . sprintf($condition, ...$values) . ')';
        } else {
            $this->where .= " OR  ($condition)";
        }
        return $this;
    }

    /**
     * Adds OR an equality condition to the "where" clause
     *
     * @param string|array $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value It'll be safety escaped
     * @return $this
     */
    public function orWhereEq(string|array $name, mixed $value): static
    {
        $this->where .= " OR  " . Helper::escapeNameUnsafe($name) . "={$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds OR a Not Equal condition to the "where" clause
     *
     * @param string|array $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function orWhereNotEq(string|array $name, mixed $value): static
    {
        $this->where .= " OR  " . Helper::escapeNameUnsafe($name) . "<>{$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds OR a Greater than condition to the "where" clause
     *
     * @param string|array $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function orWhereGt(string|array $name, mixed $value): static
    {
        $this->where .= " OR  " . Helper::escapeNameUnsafe($name) . ">{$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds OR a Less than condition to the "where" clause
     *
     * @param string|array $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function orWhereLt(string|array $name, mixed $value): static
    {
        $this->where .= " OR  " . Helper::escapeNameUnsafe($name) . "<{$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds OR a Greater than OR Equal condition to the "where" clause
     *
     * @param string|array $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function orWhereGte(string|array $name, mixed $value): static
    {
        $this->where .= " OR  " . Helper::escapeNameUnsafe($name) . ">={$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds OR a Less than OR Equal condition to the "where" clause
     *
     * @param string|array $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function orWhereLte(string|array $name, mixed $value): static
    {
        $this->where .= " OR  " . Helper::escapeNameUnsafe($name) . "<={$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds OR a like condition to the "where" clause
     *
     * @param string|array $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to match. It'll be safely escaped
     * @return $this
     */
    public function orWhereLike(string|array $name, mixed $value): static
    {
        $this->where .= " OR  " . Helper::escapeNameUnsafe($name) . " LIKE {$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds OR a not like condition to the "where" clause
     *
     * @param string|array $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to match. It'll be safely escaped
     * @return $this
     */
    public function orWhereNotLike(string|array $name, mixed $value): static
    {
        $this->where .= " OR  " . Helper::escapeNameUnsafe($name) . " NOT LIKE {$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds OR an Is Null condition to the "where" clause
     *
     * @param string|array $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @return $this
     */
    public function orWhereIsNull(string|array $name): static
    {
        $this->where .= " OR  " . Helper::escapeNameUnsafe($name) . " IS NULL";
        return $this;
    }

    /**
     * Adds OR an Isn't Null condition to the "where" clause
     *
     * @param string|array $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @return $this
     */
    public function orWhereIsNotNull(string|array $name): static
    {
        $this->where .= " OR  " . Helper::escapeNameUnsafe($name) . " IS NOT NULL";
        return $this;
    }

    /**
     * Adds OR a BETWEEN condition to the "where" clause
     *
     * @param string|array $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $start The starting value. It'll be safely escaped
     * @param mixed $end The ending value. It'll be safely escaped
     * @return $this
     */
    public function orWhereBetween(string|array $name, mixed $start, mixed $end): static
    {
        $this->where .= " OR  " . Helper::escapeNameUnsafe($name) . " BETWEEN {$this->connect->escape($start)} AND {$this->connect->escape($end)}";
        return $this;
    }

    /**
     * Adds OR an IN condition to the "where" clause
     *
     * @param string|array $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param array|Select $list The list of values or a sub query. It'll be safely escaped
     * @return $this
     */
    public function orWhereIn(string|array $name, array|Select $list): static
    {
        if (is_array($list)) {
            if (empty($list)) {
                $this->where .= " OR  FALSE";
                return $this;
            }
            foreach ($list as $k => $v) {
                $list[$k] = $this->connect->escape($v);
            }
            $list = '(' . implode(',', $list) . ')';
        } else {
            $list = "({$list->query()})";
        }

        $this->where .= " OR  " . Helper::escapeNameUnsafe($name) . " IN $list";
        return $this;
    }

    /**
     * Adds OR a not IN condition to the "where" clause
     *
     * @param string|array $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param array|Select $list The list of values or a sub query. It'll be safely escaped
     * @return $this
     */
    public function orWhereNotIn(string|array $name, array|Select $list): static
    {
        if (is_array($list)) {
            if (empty($list)) {
                $this->where .= " OR  TRUE";
                return $this;
            }
            foreach ($list as $k => $v) {
                $list[$k] = $this->connect->escape($v);
            }
            $list = '(' . implode(',', $list) . ')';
        } else {
            $list = "({$list->query()})";
        }

        $this->where .= " OR  " . Helper::escapeNameUnsafe($name) . " NOT IN $list";
        return $this;
    }

    /**
     * Adds OR an EXISTS condition to the "where" clause
     *
     * @param Select $select The sub query to check for existence
     * @return $this
     */
    public function orWhereExists(Select $select): static
    {
        $this->where .= " OR  EXISTS ({$select->query()})";
        return $this;
    }

    /**
     * Adds OR a not EXISTS condition to the "where" clause
     *
     * @param Select $select The sub query to check for non-existence
     * @return $this
     */
    public function orWhereNotExists(Select $select): static
    {
        $this->where .= " OR  NOT EXISTS ({$select->query()})";
        return $this;
    }

    /**
     * Adds OR a sub query condition to the "where" clause
     *
     * @param Where $where The sub query condition
     * @return $this
     */
    public function orWhereSubWhere(Where $where): static
    {
        $this->where .= " OR  ({$where->query()})";
        return $this;
    }

    /**
     * Adds OR a sub query condition using a closure
     * less performs then whereSubWhere
     *
     * @param Closure $sub The closure that generates a Where object
     * @return $this
     */
    public function orWhereSubFn(Closure $sub): static
    {
        $this->where .= ' OR  (' . $sub(new Where($this->connect))->query() . ')';
        return $this;
    }

    /**
     * Make a new where instance linked to same database connection
     *
     * @return Where
     */
    public function makeWhere(): Where
    {
        return new Where($this->connect);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
}