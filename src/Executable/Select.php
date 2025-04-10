<?php declare(strict_types=1);

namespace AP\Mysql\Executable;

use AP\Mysql\Connect\ConnectInterface;
use AP\Mysql\Helper;
use AP\Mysql\Raw;
use AP\Mysql\Statement\Statement;
use AP\Mysql\Statement\TableFactor;
use AP\Mysql\Statement\Where;
use Closure;
use mysqli_result;
use UnexpectedValueException;

/**
 * @see https://dev.mysql.com/doc/refman/8.4/en/select.html
 *
 * because into_option usually no use, it no included to this implementation
 * to add "into_option" you can append it after get a select query like a string
 */
class Select implements Statement, Executable
{
    private string $table;
    private string $join           = "";
    private string $window         = "";
    private string $where          = "";
    private string $having         = "";
    private string $group          = "";
    private string $order          = "";
    private ?int   $limit          = null;
    private ?int   $offset         = null;
    private bool   $distinct       = false;
    private bool   $straightJoin   = false;
    private ?bool  $sqlSmallResult = null;

    /**
     * @param ConnectInterface $connect readonly, if you need to run this query on a different connection
     *              you can run a string query on another connection,
     *              but please make sure what charset is equal,
     *              otherwise it can follow to serious security problems
     *
     * @param string|TableFactor $table
     * @param array $columns
     */
    public function __construct(
        public readonly ConnectInterface $connect,
        string|TableFactor                $table,
        private array                     $columns = [],
    )
    {
        $this->table = is_string($table) ? "`$table`" : $table->query();
    }

    protected function buildColumns(): string
    {
        if (empty($this->columns)) {
            return '*';
        }

        $columns = '';
        foreach ($this->columns as $k => $v) {
            if (is_string($v)) {
                $columns .= "`$v`";
            } elseif ($v instanceof Select) {
                $columns .= "({$v->query()})";
            } elseif ($v instanceof Raw) {
                $columns .= "{$v->escape($this->connect)}";
            } elseif (is_array($v) && isset($v[0], $v[1]) && count($v) == 2) {
                $columns .= "`$v[0]`.`$v[1]`";
            } else {
                throw new UnexpectedValueException(
                    'Column value must be string, AP\Mysql\Executable\Select or AP\Mysql\Raw'
                );
            }
            $columns .= is_string($k) ? " AS `$k`," : ',';
        }
        return substr($columns, 0, -1);
    }

    public function query(): string
    {
        return 'SELECT ' .
            ($this->distinct ? 'DISTINCT ' : '') .
            ($this->straightJoin ? 'STRAIGHT_JOIN ' : '') .
            (is_bool($this->sqlSmallResult) ? ($this->sqlSmallResult ? 'SQL_SMALL_RESULT ' : 'SQL_BIG_RESULT ') : '') .
            "{$this->buildColumns()} FROM " .
            $this->table .
            $this->join .
            (empty($this->where) ? '' : ' WHERE ' . substr($this->where, 5)) .
            (empty($this->group) ? '' : ' GROUP BY ' . substr($this->group, 1)) .
            (empty($this->having) ? '' : ' HAVING ' . substr($this->having, 5)) .
            (empty($this->window) ? '' : " WINDOW $this->window") .
            (empty($this->order) ? '' : ' ORDER BY ' . substr($this->order, 1)) .
            (is_int($this->limit) ? (' LIMIT ' . (is_int($this->offset) ? "$this->offset," : '') . "$this->limit") : '');
    }

    public function exec(): mysqli_result
    {
        return $this->connect->exec($this->query());
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public function setTable(string|TableFactor $table): static
    {
        $this->table = is_string($table) ? "`$table`" : $table->query();
        return $this;
    }

    public function setColumns(array $columns): static
    {
        $this->columns = $columns;
        return $this;
    }

    public function addColumn(string|Select|Raw|array $column, ?string $alias = null): static
    {
        if (is_null($alias)) {
            $this->columns[] = $column;
        } else {
            $this->columns[$alias] = $column;
        }
        return $this;
    }

    public function getColumns(): array
    {
        return $this->columns;
    }

    /**
     * Sets the column selection to count the total number of rows.
     *
     * This method modifies the query to use `SELECT count(*)`,
     * which returns the total row count instead of selecting specific columns.
     */
    public function setColumnCountAll(): static
    {
        $this->columns = [new Raw("count(*)")];
        return $this;
    }

    public function setLimit(?int $limit, ?int $offset = null): static
    {
        $this->limit  = $limit;
        $this->offset = $offset;
        return $this;
    }

    public function setOffset(?int $offset = null): static
    {
        $this->offset = $offset;
        return $this;
    }

    public function distinct(bool $distinct = true): static
    {
        $this->distinct = $distinct;
        return $this;
    }

    public function straightJoin(bool $straightJoin = true): static
    {
        $this->straightJoin = $straightJoin;
        return $this;
    }

    public function sqlDefResult(): static
    {
        $this->sqlSmallResult = null;
        return $this;
    }

    public function sqlSmallResult(): static
    {
        $this->sqlSmallResult = true;
        return $this;
    }

    public function sqlBigResult(): static
    {
        $this->sqlSmallResult = false;
        return $this;
    }

    public function setWindow(string $window): static
    {
        $this->window = $window;
        return $this;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private function baseJoin(
        string             $type,
        string|TableFactor $table,
        string|array|Where $on,
    ): static
    {
        $this->join .= " $type " .
            (is_string($table) ? "`$table`" : $table->query()) .
            (is_string($on)
                ? " ON $on"
                : Helper::prepareJoinOn($this->connect, ' ON', $on)
            );

        return $this;
    }

    /**
     * @param string|TableFactor $table
     * @param string|array|Where $on example: [["items", "user_id"], ["users", "id"]] -> `items`.`user_id`=`users`.`id`
     * @return $this
     */
    public function joinInner(
        string|TableFactor $table,
        string|array|Where $on,
    ): static
    {
        return $this->baseJoin('JOIN', $table, $on);
    }

    /**
     * @param string|TableFactor $table
     * @param string|array|Where $on example: [["items", "user_id"], ["users", "id"]] -> `items`.`user_id`=`users`.`id`
     * @param bool $outer
     * @return $this
     */
    public function joinLeft(
        string|TableFactor $table,
        string|array|Where $on,
        bool               $outer = false
    ): static
    {
        return $this->baseJoin('LEFT ' . ($outer ? 'OUTER ' : '') . 'JOIN', $table, $on);
    }

    /**
     * @param string|TableFactor $table
     * @param string|array|Where $on example: [["items", "user_id"], ["users", "id"]] -> `items`.`user_id`=`users`.`id`
     * @param bool $outer
     * @return $this
     */
    public function joinRight(
        string|TableFactor $table,
        string|array|Where $on,
        bool               $outer = false
    ): static
    {
        return $this->baseJoin('RIGHT ' . ($outer ? 'OUTER ' : '') . 'JOIN', $table, $on);
    }

    /**
     * @param string|TableFactor $table
     * @param string|array|Where $on example: [["items", "user_id"], ["users", "id"]] -> `items`.`user_id`=`users`.`id`
     * @return $this
     */
    public function joinStraight(
        string|TableFactor $table,
        string|array|Where $on,
    ): static
    {
        return $this->baseJoin('STRAIGHT_JOIN', $table, $on);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Adds a group condition to the "group by" clause
     *
     * @param int|string $name The expression used for grouping
     * @return $this
     */
    public function group(int|string $name): static
    {
        $this->group .= is_string($name) ? ",`$name`" : ",$name";
        return $this;
    }

    /**
     * Adds an expression-based group condition to the "group by" clause
     *
     * @param string $expr The expression used for grouping
     * @return $this
     */
    public function groupExpr(string $expr): static
    {
        $this->group .= ",$expr";
        return $this;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Adds an ascending order condition to the "order by" clause
     *
     * @param string|int $name The column name or index to order by. The name will be just wrapped in backticks
     *                         Don't use raw user input directly to form column names.
     *                         If ordered by an aliased column, write it as o`.`column to get `o`.`column`
     * @return $this
     */
    public function order(string|int $name): static
    {
        $this->order .= is_string($name) ? ",`$name`" : ",$name";
        return $this;
    }

    /**
     * Adds a descending order condition to the "order by" clause
     *
     * @param string|int $name The column name or index to order by. The name will be just wrapped in backticks
     *                         Don't use raw user input directly to form column names.
     *                         If ordered by an aliased column, write it as o`.`column to get `o`.`column`
     * @return $this
     */
    public function orderDesc(string|int $name): static
    {
        $this->order .= is_string($name) ? ",`$name` DESC" : ",$name DESC";
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
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value It'll be safety escaped
     * @return $this
     */
    public function whereEq(string $name, mixed $value): static
    {
        $this->where .= " AND `$name`={$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds "And" a Not Equal condition to the "where" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function whereNotEq(string $name, mixed $value): static
    {
        $this->where .= " AND `$name`<>{$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds "And" a Greater than condition to the "where" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function whereGt(string $name, mixed $value): static
    {
        $this->where .= " AND `$name`>{$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds "And" a Less than condition to the "where" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function whereLt(string $name, mixed $value): static
    {
        $this->where .= " AND `$name`<{$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds "And" a Greater than OR Equal condition to the "where" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function whereGte(string $name, mixed $value): static
    {
        $this->where .= " AND `$name`>={$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds "And" a Less than OR Equal condition to the "where" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function whereLte(string $name, mixed $value): static
    {
        $this->where .= " AND `$name`<={$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds "And" a like condition to the "where" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to match. It'll be safely escaped
     * @return $this
     */
    public function whereLike(string $name, mixed $value): static
    {
        $this->where .= " AND `$name` LIKE {$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds "And" a not like condition to the "where" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to match. It'll be safely escaped
     * @return $this
     */
    public function whereNotLike(string $name, mixed $value): static
    {
        $this->where .= " AND `$name` NOT LIKE {$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds "And" Is Null condition to the "where" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @return $this
     */
    public function whereIsNull(string $name): static
    {
        $this->where .= " AND `$name` IS NULL";
        return $this;
    }

    /**
     * Adds "And" Isn't Null condition to the "where" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @return $this
     */
    public function whereIsNotNull(string $name): static
    {
        $this->where .= " AND `$name` IS NOT NULL";
        return $this;
    }

    /**
     * Adds "And" a BETWEEN condition to the "where" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $start The starting value. It'll be safely escaped
     * @param mixed $end The ending value. It'll be safely escaped
     * @return $this
     */
    public function whereBetween(string $name, mixed $start, mixed $end): static
    {
        $this->where .= " AND `$name` BETWEEN {$this->connect->escape($start)} AND {$this->connect->escape($end)}";
        return $this;
    }

    /**
     * Adds "And" an IN condition to the "where" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param array|Select $list The list of values or a sub query. It'll be safely escaped
     * @return $this
     */
    public function whereIn(string $name, array|Select $list): static
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

        $this->where .= " AND `$name` IN $list";
        return $this;
    }

    /**
     * Adds "And" a not IN condition to the "where" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param array|Select $list The list of values or a sub query. It'll be safely escaped
     * @return $this
     */
    public function whereNotIn(string $name, array|Select $list): static
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

        $this->where .= " AND `$name` NOT IN $list";
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
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value It'll be safety escaped
     * @return $this
     */
    public function orWhereEq(string $name, mixed $value): static
    {
        $this->where .= " OR  `$name`={$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds OR a Not Equal condition to the "where" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function orWhereNotEq(string $name, mixed $value): static
    {
        $this->where .= " OR  `$name`<>{$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds OR a Greater than condition to the "where" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function orWhereGt(string $name, mixed $value): static
    {
        $this->where .= " OR  `$name`>{$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds OR a Less than condition to the "where" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function orWhereLt(string $name, mixed $value): static
    {
        $this->where .= " OR  `$name`<{$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds OR a Greater than OR Equal condition to the "where" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function orWhereGte(string $name, mixed $value): static
    {
        $this->where .= " OR  `$name`>={$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds OR a Less than OR Equal condition to the "where" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function orWhereLte(string $name, mixed $value): static
    {
        $this->where .= " OR  `$name`<={$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds OR a like condition to the "where" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to match. It'll be safely escaped
     * @return $this
     */
    public function orWhereLike(string $name, mixed $value): static
    {
        $this->where .= " OR  `$name` LIKE {$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds OR a not like condition to the "where" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to match. It'll be safely escaped
     * @return $this
     */
    public function orWhereNotLike(string $name, mixed $value): static
    {
        $this->where .= " OR  `$name` NOT LIKE {$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds OR an Is Null condition to the "where" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @return $this
     */
    public function orWhereIsNull(string $name): static
    {
        $this->where .= " OR  `$name` IS NULL";
        return $this;
    }

    /**
     * Adds OR an Isn't Null condition to the "where" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @return $this
     */
    public function orWhereIsNotNull(string $name): static
    {
        $this->where .= " OR  `$name` IS NOT NULL";
        return $this;
    }

    /**
     * Adds OR a BETWEEN condition to the "where" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $start The starting value. It'll be safely escaped
     * @param mixed $end The ending value. It'll be safely escaped
     * @return $this
     */
    public function orWhereBetween(string $name, mixed $start, mixed $end): static
    {
        $this->where .= " OR  `$name` BETWEEN {$this->connect->escape($start)} AND {$this->connect->escape($end)}";
        return $this;
    }

    /**
     * Adds OR an IN condition to the "where" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param array|Select $list The list of values or a sub query. It'll be safely escaped
     * @return $this
     */
    public function orWhereIn(string $name, array|Select $list): static
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

        $this->where .= " OR  `$name` IN $list";
        return $this;
    }

    /**
     * Adds OR a not IN condition to the "where" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param array|Select $list The list of values or a sub query. It'll be safely escaped
     * @return $this
     */
    public function orWhereNotIn(string $name, array|Select $list): static
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

        $this->where .= " OR  `$name` NOT IN $list";
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

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Adds "And" a formatted condition to the "having" clause.
     *
     * @param string $condition The condition string, formatted using sprintf
     * * @param mixed ...$values Values to be inserted into the condition
     * *                         Each value will be properly escaped to ensure MySQL safety
     * @return $this
     */
    public function havingCond(string $condition, mixed ...$values): static
    {
        if (!empty($values)) {
            foreach ($values as $k => &$v) {
                $values[$k] = $this->connect->escape($v);
            }
            $this->having .= ' AND (' . sprintf($condition, ...$values) . ')';
        } else {
            $this->having .= " AND ($condition)";
        }
        return $this;
    }

    /**
     * Adds "And" an equality condition to the "having" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value It'll be safety escaped
     * @return $this
     */
    public function havingEq(string $name, mixed $value): static
    {
        $this->having .= " AND `$name`={$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds "And" a Not Equal condition to the "having" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function havingNotEq(string $name, mixed $value): static
    {
        $this->having .= " AND `$name`<>{$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds "And" a Greater than condition to the "having" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function havingGt(string $name, mixed $value): static
    {
        $this->having .= " AND `$name`>{$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds "And" a Less than condition to the "having" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function havingLt(string $name, mixed $value): static
    {
        $this->having .= " AND `$name`<{$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds "And" a Greater than OR Equal condition to the "having" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function havingGte(string $name, mixed $value): static
    {
        $this->having .= " AND `$name`>={$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds "And" a Less than OR Equal condition to the "having" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function havingLte(string $name, mixed $value): static
    {
        $this->having .= " AND `$name`<={$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds "And" a like condition to the "having" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to match. It'll be safely escaped
     * @return $this
     */
    public function havingLike(string $name, mixed $value): static
    {
        $this->having .= " AND `$name` LIKE {$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds "And" a not like condition to the "having" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to match. It'll be safely escaped
     * @return $this
     */
    public function havingNotLike(string $name, mixed $value): static
    {
        $this->having .= " AND `$name` NOT LIKE {$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds "And" Is Null condition to the "having" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @return $this
     */
    public function havingIsNull(string $name): static
    {
        $this->having .= " AND `$name` IS NULL";
        return $this;
    }

    /**
     * Adds "And" Isn't Null condition to the "having" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @return $this
     */
    public function havingIsNotNull(string $name): static
    {
        $this->having .= " AND `$name` IS NOT NULL";
        return $this;
    }

    /**
     * Adds "And" a BETWEEN condition to the "having" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $start The starting value. It'll be safely escaped
     * @param mixed $end The ending value. It'll be safely escaped
     * @return $this
     */
    public function havingBetween(string $name, mixed $start, mixed $end): static
    {
        $this->having .= " AND `$name` BETWEEN {$this->connect->escape($start)} AND {$this->connect->escape($end)}";
        return $this;
    }

    /**
     * Adds "And" an IN condition to the "having" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param array|Select $list The list of values or a sub query. It'll be safely escaped
     * @return $this
     */
    public function havingIn(string $name, array|Select $list): static
    {
        if (is_array($list)) {
            foreach ($list as $k => $v) {
                $list[$k] = $this->connect->escape($v);
            }
            $list = '(' . implode(',', $list) . ')';
        } else {
            $list = "({$list->query()})";
        }

        $this->having .= " AND `$name` IN $list";
        return $this;
    }

    /**
     * Adds "And" a not IN condition to the "having" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param array|Select $list The list of values or a sub query. It'll be safely escaped
     * @return $this
     */
    public function havingNotIn(string $name, array|Select $list): static
    {
        if (is_array($list)) {
            foreach ($list as $k => $v) {
                $list[$k] = $this->connect->escape($v);
            }
            $list = '(' . implode(',', $list) . ')';
        } else {
            $list = "({$list->query()})";
        }

        $this->having .= " AND `$name` NOT IN $list";
        return $this;
    }

    /**
     * Adds "And" an EXISTS condition to the "having" clause
     *
     * @param Select $select The sub query to check for existence
     * @return $this
     */
    public function havingExists(Select $select): static
    {
        $this->having .= " AND EXISTS ({$select->query()})";
        return $this;
    }

    /**
     * Adds "And" a not EXISTS condition to the "having" clause
     *
     * @param Select $select The sub query to check for non-existence
     * @return $this
     */
    public function havingNotExists(Select $select): static
    {
        $this->having .= " AND NOT EXISTS ({$select->query()})";
        return $this;
    }

    /**
     * Adds "And" a sub query condition to the "having" clause
     *
     * @param Where $where The sub query condition
     * @return $this
     */
    public function havingSubWhere(Where $where): static
    {
        $this->having .= " AND ({$where->query()})";
        return $this;
    }

    /**
     * Adds "And" a sub query condition using a closure
     * less performs then whereSubWhere
     *
     * @param Closure $sub The closure that generates a Where object
     * @return $this
     */
    public function havingSubFn(Closure $sub): static
    {
        $this->having .= ' AND (' . $sub(new Where($this->connect))->query() . ')';
        return $this;
    }

    /**
     * Adds OR a formatted condition to the "having" clause.
     *
     * @param string $condition The condition string, formatted using sprintf
     * @param mixed ...$values Values to be inserted into the condition
     *                         Each value will be properly escaped to ensure MySQL safety
     * @return $this
     */
    public function orHavingCond(string $condition, mixed ...$values): static
    {
        if (!empty($values)) {
            foreach ($values as $k => &$v) {
                $values[$k] = $this->connect->escape($v);
            }
            $this->having .= ' OR  (' . sprintf($condition, ...$values) . ')';
        } else {
            $this->having .= " OR  ($condition)";
        }
        return $this;
    }

    /**
     * Adds OR an equality condition to the "having" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value It'll be safety escaped
     * @return $this
     */
    public function orHavingEq(string $name, mixed $value): static
    {
        $this->having .= " OR  `$name`={$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds OR a Not Equal condition to the "having" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function orHavingNotEq(string $name, mixed $value): static
    {
        $this->having .= " OR  `$name`<>{$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds OR a Greater than condition to the "having" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function orHavingGt(string $name, mixed $value): static
    {
        $this->having .= " OR  `$name`>{$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds OR a Less than condition to the "having" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function orHavingLt(string $name, mixed $value): static
    {
        $this->having .= " OR  `$name`<{$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds OR a Greater than OR Equal condition to the "having" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function orHavingGte(string $name, mixed $value): static
    {
        $this->having .= " OR  `$name`>={$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds OR a Less than OR Equal condition to the "having" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to compare. It'll be safely escaped
     * @return $this
     */
    public function orHavingLte(string $name, mixed $value): static
    {
        $this->having .= " OR  `$name`<={$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds OR a like condition to the "having" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to match. It'll be safely escaped
     * @return $this
     */
    public function orHavingLike(string $name, mixed $value): static
    {
        $this->having .= " OR  `$name` LIKE {$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds OR a not like condition to the "having" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $value The value to match. It'll be safely escaped
     * @return $this
     */
    public function orHavingNotLike(string $name, mixed $value): static
    {
        $this->having .= " OR  `$name` NOT LIKE {$this->connect->escape($value)}";
        return $this;
    }

    /**
     * Adds OR an Is Null condition to the "having" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @return $this
     */
    public function orHavingIsNull(string $name): static
    {
        $this->having .= " OR  `$name` IS NULL";
        return $this;
    }

    /**
     * Adds OR an Isn't Null condition to the "having" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @return $this
     */
    public function orHavingIsNotNull(string $name): static
    {
        $this->having .= " OR  `$name` IS NOT NULL";
        return $this;
    }

    /**
     * Adds OR a BETWEEN condition to the "having" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param mixed $start The starting value. It'll be safely escaped
     * @param mixed $end The ending value. It'll be safely escaped
     * @return $this
     */
    public function orHavingBetween(string $name, mixed $start, mixed $end): static
    {
        $this->having .= " OR  `$name` BETWEEN {$this->connect->escape($start)} AND {$this->connect->escape($end)}";
        return $this;
    }

    /**
     * Adds OR an IN condition to the "having" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param array|Select $list The list of values or a sub query. It'll be safely escaped
     * @return $this
     */
    public function orHavingIn(string $name, array|Select $list): static
    {
        if (is_array($list)) {
            foreach ($list as $k => $v) {
                $list[$k] = $this->connect->escape($v);
            }
            $list = '(' . implode(',', $list) . ')';
        } else {
            $list = "({$list->query()})";
        }

        $this->having .= " OR  `$name` IN $list";
        return $this;
    }

    /**
     * Adds OR a not IN condition to the "having" clause
     *
     * @param string $name The column name. Don't use raw user input to form the column name
     *                     As it's unsafe for performance reasons
     *                     If needed, use AP\Mysql\Helper::escapeName() to sanitize it
     *                     If using a table or alias, write it as o`.`column to get `o`.`column`
     * @param array|Select $list The list of values or a sub query. It'll be safely escaped
     * @return $this
     */
    public function orHavingNotIn(string $name, array|Select $list): static
    {
        if (is_array($list)) {
            foreach ($list as $k => $v) {
                $list[$k] = $this->connect->escape($v);
            }
            $list = '(' . implode(',', $list) . ')';
        } else {
            $list = "({$list->query()})";
        }

        $this->having .= " OR  `$name` NOT IN $list";
        return $this;
    }

    /**
     * Adds OR an EXISTS condition to the "having" clause
     *
     * @param Select $select The sub query to check for existence
     * @return $this
     */
    public function orHavingExists(Select $select): static
    {
        $this->having .= " OR  EXISTS ({$select->query()})";
        return $this;
    }

    /**
     * Adds OR a not EXISTS condition to the "having" clause
     *
     * @param Select $select The sub query to check for non-existence
     * @return $this
     */
    public function orHavingNotExists(Select $select): static
    {
        $this->having .= " OR  NOT EXISTS ({$select->query()})";
        return $this;
    }

    /**
     * Adds OR a sub query condition to the "having" clause
     *
     * @param Where $where The sub query condition
     * @return $this
     */
    public function orHavingSubWhere(Where $where): static
    {
        $this->having .= " OR  ({$where->query()})";
        return $this;
    }

    /**
     * Adds OR a sub query condition using a closure
     * less performs then whereSubWhere
     *
     * @param Closure $sub The closure that generates a Where object
     * @return $this
     */
    public function orHavingSubFn(Closure $sub): static
    {
        $this->having .= ' OR  (' . $sub(new Where($this->connect))->query() . ')';
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

    /**
     * Fetch all rows from the executed query result as an associative array.
     *
     * @return array The result set as an array of associative arrays.
     */
    public function fetchAll(): array
    {
        return $this->exec()->fetch_all(MYSQLI_ASSOC);
    }

    /**
     * Fetch a single row from the executed query result as an associative array.
     *
     * @return ?array The first row of the result set as an associative array.
     */
    public function fetchRow(): ?array
    {
        return $this->exec()->fetch_assoc();
    }

    /**
     * Fetch a single column from all rows of the executed query result.
     *
     * @return array An array containing the values from the first column of each row.
     */
    public function fetchCol(): array
    {
        return array_column(
            $this->exec()->fetch_all(MYSQLI_NUM),
            0
        );
    }

    /**
     * Fetch key-value pairs from the executed query result.
     *
     * The query must return exactly two columns: the first column as keys and the second as values.
     *
     * @return array An associative array where the first column is used as keys and the second as values.
     * @throws \RuntimeException If the query does not return exactly two columns.
     */
    public function fetchPairs(): array
    {
        $res = $this->exec();

        $array = [];

        if ($res->field_count == 2) {
            while ($el = $res->fetch_row()) {
                $array[$el[0]] = $el[1];
            }
        } else {
            throw new \RuntimeException(
                'Invalid query for fetchPairs, use only 2 fields'
            );
        }

        return $array;
    }

    /**
     * Fetch unique rows based on the first column value.
     *
     * The first column is used as the key, and the corresponding row is stored as the value.
     * If a duplicate key is encountered, it is ignored.
     *
     * @return array An associative array where the first column value is used as the key,
     *               and the corresponding row (associative array) is the value.
     */
    public function fetchUnique(): array
    {
        $res   = $this->exec();
        $array = [];
        while ($el = $res->fetch_assoc()) {
            $array[current($el)] = $el;
        }
        return $array;
    }

    /**
     * Fetch a single value from the first column of the first row in the result set.
     *
     * If the query returns no rows, the provided default value is returned instead.
     *
     * @param mixed $default The default value to return if no rows are found. Defaults to `false`.
     * @return mixed The first column's value from the first row, or the default value if no results exist.
     */
    public function fetchOne($default = false)
    {
        $res = $this->exec();
        return $res->num_rows ?
            $res->fetch_row()[0] :
            $default;
    }

    public function fetchCountAll(): int
    {
        return (int)(clone $this)
            ->setColumnCountAll()
            ->fetchOne();
    }
}