<?php declare(strict_types=1);

namespace AP\Mysql\Executable;

use AP\Mysql\Connect\ConnectInterface;
use AP\Mysql\Helper;
use AP\Mysql\Raw;
use AP\Mysql\Statement\GroupBy;
use AP\Mysql\Statement\OrderBy;
use AP\Mysql\Statement\Statement;
use AP\Mysql\Statement\TableFactor;
use AP\Mysql\Statement\Where;
use Closure;
use mysqli_result;

/**
 * @see https://dev.mysql.com/doc/refman/8.4/en/select.html
 */
class Select implements Statement, Executable
{
    private bool  $straightJoin   = false;
    private ?bool $sqlSmallResult = null;

    // TODO: add joins
    // TODO: add window

    /**
     * @param ConnectInterface $connect readonly, if you need to run this query on a different connection
     *              you can run ```$DifferentConnection->exec($originalSelect->query())```
     *              but please make sure what charset is equal,
     *              otherwise it can follow to serious security problems
     *
     * @param string|TableFactor $table
     * @param array $columns
     * @param Where|array|null $where
     * @param Where|array|null $having
     * @param GroupBy|string|null $group
     * @param OrderBy|string|null $order
     * @param int|null $limit
     * @param int|null $offset
     * @param bool $distinct
     */
    public function __construct(
        private readonly ConnectInterface $connect,
        private string|TableFactor        $table,
        private array                     $columns = [],
        private Where|array|null          $where = null,
        private Where|array|null          $having = null,
        private GroupBy|string|null       $group = null,
        private OrderBy|string|null       $order = null,
        private ?int                      $limit = null,
        private ?int                      $offset = null,
        private bool                      $distinct = false,
    )
    {
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
            } else {
                throw new \UnexpectedValueException(
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
            (is_string($this->table) ? "`$this->table`" : $this->table->query()) .
            Helper::prepareWhere($this->connect, " WHERE", $this->where) .
            (is_string($this->group)
                ? " GROUP BY $this->group"
                : ($this->group instanceof GroupBy ? " GROUP BY {$this->group->query()}" : '')
            ) .
            Helper::prepareWhere($this->connect, " HAVING", $this->having) .
            // add WINDOW SPEC
            (is_string($this->order)
                ? " ORDER BY $this->order"
                : ($this->order instanceof OrderBy ? " ORDER BY {$this->order->query()}" : '')
            ) .
            (is_int($this->limit) ? (" LIMIT $this->limit" . (is_int($this->offset) ? ",$this->offset" : '')) : '');
    }

    public function exec(): mysqli_result
    {
        return $this->connect->exec($this->query());
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public function setTable(string|TableFactor $table): static
    {
        $this->table = $table;
        return $this;
    }

    public function setColumns(array $columns): static
    {
        $this->columns = $columns;
        return $this;
    }

    public function setLimit(?int $limit, ?int $offset = null): static
    {
        $this->limit  = $limit;
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

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public function groupByObject(): GroupBy
    {
        if (is_null($this->group)) {
            $this->group = new GroupBy();
        } elseif (is_string($this->group)) {
            $this->group = (new GroupBy)->expr($this->group);
        }
        return $this->group;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public function orderBy(): OrderBy
    {
        if (is_null($this->order)) {
            $this->order = new OrderBy();
        } elseif (is_string($this->order)) {
            $this->order = (new OrderBy)->expr($this->order);
        }
        return $this->order;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


    /**
     * use it only if you want to add changes to where, because it always will convert to Where object
     * it can reduce performance
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

    public function whereCond(string $condition, mixed ...$values): static
    {
        $this->getWhereObject()->cond($condition, ...$values);
        return $this;
    }

    public function whereEq(string $name, mixed $value): static
    {
        $this->getWhereObject()->eq($name, $value);
        return $this;
    }

    public function whereNotEq(string $name, mixed $value): static
    {
        $this->getWhereObject()->notEq($name, $value);
        return $this;
    }

    public function whereGt(string $name, mixed $value): static
    {
        $this->getWhereObject()->gt($name, $value);
        return $this;
    }

    public function whereLt(string $name, mixed $value): static
    {
        $this->getWhereObject()->lt($name, $value);
        return $this;
    }

    public function whereGte(string $name, mixed $value): static
    {
        $this->getWhereObject()->gte($name, $value);
        return $this;
    }

    public function whereLte(string $name, mixed $value): static
    {
        $this->getWhereObject()->lte($name, $value);
        return $this;
    }

    public function whereLike(string $name, mixed $value): static
    {
        $this->getWhereObject()->like($name, $value);
        return $this;
    }

    public function whereNotLike(string $name, mixed $value): static
    {
        $this->getWhereObject()->notLike($name, $value);
        return $this;
    }

    public function whereIsNull(string $name): static
    {
        $this->getWhereObject()->isNull($name);
        return $this;
    }

    public function whereIsNotNull(string $name): static
    {
        $this->getWhereObject()->isNotNull($name);
        return $this;
    }

    public function whereBetween(string $name, mixed $start, mixed $end): static
    {
        $this->getWhereObject()->between($name, $start, $end);
        return $this;
    }

    public function whereIn(string $name, array|Select $list): static
    {
        $this->getWhereObject()->in($name, $list);
        return $this;
    }

    public function whereNotIn(string $name, array|Select $list): static
    {
        $this->getWhereObject()->notIn($name, $list);
        return $this;
    }

    public function whereExists(Select $select): static
    {
        $this->getWhereObject()->exists($select);
        return $this;
    }

    public function whereNotExists(Select $select): static
    {
        $this->getWhereObject()->notExists($select);
        return $this;
    }

    public function whereSubWhere(Where $where): static
    {
        $this->getWhereObject()->subWhere($where);
        return $this;
    }

    public function whereSubFn(Closure $sub): static
    {
        $this->getWhereObject()->subFn($sub);
        return $this;
    }

    public function orWhereCond(string $condition, mixed ...$values): static
    {
        $this->getWhereObject()->orCond($condition, ...$values);
        return $this;
    }

    public function orWhereEq(string $name, mixed $value): static
    {
        $this->getWhereObject()->orEq($name, $value);
        return $this;
    }

    public function orWhereNotEq(string $name, mixed $value): static
    {
        $this->getWhereObject()->orNotEq($name, $value);
        return $this;
    }

    public function orWhereGt(string $name, mixed $value): static
    {
        $this->getWhereObject()->orGt($name, $value);
        return $this;
    }

    public function orWhereLt(string $name, mixed $value): static
    {
        $this->getWhereObject()->orLt($name, $value);
        return $this;
    }

    public function orWhereGte(string $name, mixed $value): static
    {
        $this->getWhereObject()->orGte($name, $value);
        return $this;
    }

    public function orWhereLte(string $name, mixed $value): static
    {
        $this->getWhereObject()->orLte($name, $value);
        return $this;
    }

    public function orWhereLike(string $name, mixed $value): static
    {
        $this->getWhereObject()->orLike($name, $value);
        return $this;
    }

    public function orWhereNotLike(string $name, mixed $value): static
    {
        $this->getWhereObject()->orNotLike($name, $value);
        return $this;
    }

    public function orWhereIsNull(string $name): static
    {
        $this->getWhereObject()->orIsNull($name);
        return $this;
    }

    public function orWhereIsNotNull(string $name): static
    {
        $this->getWhereObject()->orIsNotNull($name);
        return $this;
    }

    public function orWhereBetween(string $name, mixed $start, mixed $end): static
    {
        $this->getWhereObject()->orBetween($name, $start, $end);
        return $this;
    }

    public function orWhereIn(string $name, array|Select $list): static
    {
        $this->getWhereObject()->orIn($name, $list);
        return $this;
    }

    public function orWhereNotIn(string $name, array|Select $list): static
    {
        $this->getWhereObject()->orNotIn($name, $list);
        return $this;
    }

    public function orWhereExists(Select $select): static
    {
        $this->getWhereObject()->orExists($select);
        return $this;
    }

    public function orWhereNotExists(Select $select): static
    {
        $this->getWhereObject()->orNotExists($select);
        return $this;
    }

    public function orWhereSubWhere(Where $where): static
    {
        $this->getWhereObject()->orSubWhere($where);
        return $this;
    }

    public function orWhereSubFn(Closure $sub): static
    {
        $this->getWhereObject()->orSubFn($sub);
        return $this;
    }


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * use it only if you want to add changes to where, because it always will convert to Where object
     * it can reduce performance
     *
     * @return Where
     */
    public function getHavingObject(): Where
    {
        if (is_null($this->having)) {
            $this->having = new Where($this->connect);
        } elseif (is_array($this->having)) {
            $where = new Where($this->connect);
            foreach ($this->having as $k => $v) {
                $where->eq($k, $v);
            }
            $this->having = $where;
        }
        return $this->having;
    }

    public function havingCond(string $condition, mixed ...$values): static
    {
        $this->getHavingObject()->cond($condition, ...$values);
        return $this;
    }

    public function havingEq(string $name, mixed $value): static
    {
        $this->getHavingObject()->eq($name, $value);
        return $this;
    }

    public function havingNotEq(string $name, mixed $value): static
    {
        $this->getHavingObject()->notEq($name, $value);
        return $this;
    }

    public function havingGt(string $name, mixed $value): static
    {
        $this->getHavingObject()->gt($name, $value);
        return $this;
    }

    public function havingLt(string $name, mixed $value): static
    {
        $this->getHavingObject()->lt($name, $value);
        return $this;
    }

    public function havingGte(string $name, mixed $value): static
    {
        $this->getHavingObject()->gte($name, $value);
        return $this;
    }

    public function havingLte(string $name, mixed $value): static
    {
        $this->getHavingObject()->lte($name, $value);
        return $this;
    }

    public function havingLike(string $name, mixed $value): static
    {
        $this->getHavingObject()->like($name, $value);
        return $this;
    }

    public function havingNotLike(string $name, mixed $value): static
    {
        $this->getHavingObject()->notLike($name, $value);
        return $this;
    }

    public function havingIsNull(string $name): static
    {
        $this->getHavingObject()->isNull($name);
        return $this;
    }

    public function havingIsNotNull(string $name): static
    {
        $this->getHavingObject()->isNotNull($name);
        return $this;
    }

    public function havingBetween(string $name, mixed $start, mixed $end): static
    {
        $this->getHavingObject()->between($name, $start, $end);
        return $this;
    }

    public function havingIn(string $name, array|Select $list): static
    {
        $this->getHavingObject()->in($name, $list);
        return $this;
    }

    public function havingNotIn(string $name, array|Select $list): static
    {
        $this->getHavingObject()->notIn($name, $list);
        return $this;
    }

    public function havingExists(Select $select): static
    {
        $this->getHavingObject()->exists($select);
        return $this;
    }

    public function havingNotExists(Select $select): static
    {
        $this->getHavingObject()->notExists($select);
        return $this;
    }

    public function havingSubWhere(Where $where): static
    {
        $this->getHavingObject()->subWhere($where);
        return $this;
    }

    public function havingSubFn(Closure $sub): static
    {
        $this->getHavingObject()->subFn($sub);
        return $this;
    }

    public function orHavingCond(string $condition, mixed ...$values): static
    {
        $this->getHavingObject()->orCond($condition, ...$values);
        return $this;
    }

    public function orHavingEq(string $name, mixed $value): static
    {
        $this->getHavingObject()->orEq($name, $value);
        return $this;
    }

    public function orHavingNotEq(string $name, mixed $value): static
    {
        $this->getHavingObject()->orNotEq($name, $value);
        return $this;
    }

    public function orHavingGt(string $name, mixed $value): static
    {
        $this->getHavingObject()->orGt($name, $value);
        return $this;
    }

    public function orHavingLt(string $name, mixed $value): static
    {
        $this->getHavingObject()->orLt($name, $value);
        return $this;
    }

    public function orHavingGte(string $name, mixed $value): static
    {
        $this->getHavingObject()->orGte($name, $value);
        return $this;
    }

    public function orHavingLte(string $name, mixed $value): static
    {
        $this->getHavingObject()->orLte($name, $value);
        return $this;
    }

    public function orHavingLike(string $name, mixed $value): static
    {
        $this->getHavingObject()->orLike($name, $value);
        return $this;
    }

    public function orHavingNotLike(string $name, mixed $value): static
    {
        $this->getHavingObject()->orNotLike($name, $value);
        return $this;
    }

    public function orHavingIsNull(string $name): static
    {
        $this->getHavingObject()->orIsNull($name);
        return $this;
    }

    public function orHavingIsNotNull(string $name): static
    {
        $this->getHavingObject()->orIsNotNull($name);
        return $this;
    }

    public function orHavingBetween(string $name, mixed $start, mixed $end): static
    {
        $this->getHavingObject()->orBetween($name, $start, $end);
        return $this;
    }

    public function orHavingIn(string $name, array|Select $list): static
    {
        $this->getHavingObject()->orIn($name, $list);
        return $this;
    }

    public function orHavingNotIn(string $name, array|Select $list): static
    {
        $this->getHavingObject()->orNotIn($name, $list);
        return $this;
    }

    public function orHavingExists(Select $select): static
    {
        $this->getHavingObject()->orExists($select);
        return $this;
    }

    public function orHavingNotExists(Select $select): static
    {
        $this->getHavingObject()->orNotExists($select);
        return $this;
    }

    public function orHavingSubWhere(Where $where): static
    {
        $this->getHavingObject()->orSubWhere($where);
        return $this;
    }

    public function orHavingSubFn(Closure $sub): static
    {
        $this->getHavingObject()->orSubFn($sub);
        return $this;
    }
}