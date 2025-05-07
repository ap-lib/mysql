<?php declare(strict_types=1);

namespace AP\Mysql\Statement;

use AP\Mysql\Connect\ConnectInterface;
use AP\Mysql\Executable\Select;
use AP\Mysql\Helper;
use Closure;

class Where implements Statement
{
    private string $where = '';

    /**
     * @param ConnectInterface $connect
     */
    public function __construct(
        public readonly ConnectInterface $connect,
    )
    {
    }

    /**
     * Be performance reason after OR always going two spaces, after AND only one
     * @return string blank page if no where
     */
    public function query(): string
    {
        return substr($this->where, 5);
    }

    private function escapeList(array|Select $list): string
    {
        if (is_array($list)) {
            foreach ($list as $k => $v) {
                $list[$k] = $this->connect->escape($v);
            }
            return '(' . implode(',', $list) . ')';
        }

        return "({$list->query()})";
    }

    //////////////////////////////////////

    public function cond(string $condition, mixed ...$values): static
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

    public function eq(string|array $name, mixed $value): static
    {
        $this->where .= " AND " . Helper::escapeNameUnsafe($name) . "={$this->connect->escape($value)}";
        return $this;
    }

    public function notEq(string|array $name, mixed $value): static
    {
        $this->where .= " AND " . Helper::escapeNameUnsafe($name) . "<>{$this->connect->escape($value)}";
        return $this;
    }

    public function gt(string|array $name, mixed $value): static
    {
        $this->where .= " AND " . Helper::escapeNameUnsafe($name) . ">{$this->connect->escape($value)}";
        return $this;
    }

    public function lt(string|array $name, mixed $value): static
    {
        $this->where .= " AND " . Helper::escapeNameUnsafe($name) . "<{$this->connect->escape($value)}";
        return $this;
    }

    public function gte(string|array $name, mixed $value): static
    {
        $this->where .= " AND " . Helper::escapeNameUnsafe($name) . ">={$this->connect->escape($value)}";
        return $this;
    }

    public function lte(string|array $name, mixed $value): static
    {
        $this->where .= " AND " . Helper::escapeNameUnsafe($name) . "<={$this->connect->escape($value)}";
        return $this;
    }

    public function like(string|array $name, mixed $value): static
    {
        $this->where .= " AND " . Helper::escapeNameUnsafe($name) . " LIKE {$this->connect->escape($value)}";
        return $this;
    }

    public function notLike(string|array $name, mixed $value): static
    {
        $this->where .= " AND " . Helper::escapeNameUnsafe($name) . " NOT LIKE {$this->connect->escape($value)}";
        return $this;
    }

    public function isNull(string|array $name): static
    {
        $this->where .= " AND " . Helper::escapeNameUnsafe($name) . " IS NULL";
        return $this;
    }

    public function isNotNull(string|array $name): static
    {
        $this->where .= " AND " . Helper::escapeNameUnsafe($name) . " IS NOT NULL";
        return $this;
    }

    public function between(string|array $name, mixed $start, mixed $end): static
    {
        $this->where .= " AND " . Helper::escapeNameUnsafe($name) . " BETWEEN {$this->connect->escape($start)} AND {$this->connect->escape($end)}";
        return $this;
    }

    public function in(string|array $name, array|Select $list): static
    {
        $this->where .= " AND " . Helper::escapeNameUnsafe($name) . " IN {$this->escapeList($list)}";
        return $this;
    }

    public function notIn(string|array $name, array|Select $list): static
    {
        $this->where .= " AND " . Helper::escapeNameUnsafe($name) . " NOT IN {$this->escapeList($list)}";
        return $this;
    }

    public function exists(Select $select): static
    {
        $this->where .= " AND EXISTS ({$select->query()})";
        return $this;
    }

    public function notExists(Select $select): static
    {
        $this->where .= " AND NOT EXISTS ({$select->query()})";
        return $this;
    }

    public function subWhere(Where $where): static
    {
        $this->where .= " AND ({$where->query()})";
        return $this;
    }

    public function subFn(Closure $sub): static
    {
        $this->where .= ' AND (' . $sub(new Where($this->connect))->query() . ')';
        return $this;
    }

    //////////////////////////////////////

    public function orCond(string $condition, mixed ...$values): static
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

    public function orEq(string|array $name, mixed $value): static
    {
        $this->where .= " OR  " . Helper::escapeNameUnsafe($name) . "={$this->connect->escape($value)}";
        return $this;
    }

    public function orNotEq(string|array $name, mixed $value): static
    {
        $this->where .= " OR  " . Helper::escapeNameUnsafe($name) . "<>{$this->connect->escape($value)}";
        return $this;
    }

    public function orGt(string|array $name, mixed $value): static
    {
        $this->where .= " OR  " . Helper::escapeNameUnsafe($name) . ">{$this->connect->escape($value)}";
        return $this;
    }

    public function orLt(string|array $name, mixed $value): static
    {
        $this->where .= " OR  " . Helper::escapeNameUnsafe($name) . "<{$this->connect->escape($value)}";
        return $this;
    }

    public function orGte(string|array $name, mixed $value): static
    {
        $this->where .= " OR  " . Helper::escapeNameUnsafe($name) . ">={$this->connect->escape($value)}";
        return $this;
    }

    public function orLte(string|array $name, mixed $value): static
    {
        $this->where .= " OR  " . Helper::escapeNameUnsafe($name) . "<={$this->connect->escape($value)}";
        return $this;
    }

    public function orLike(string|array $name, mixed $value): static
    {
        $this->where .= " OR  " . Helper::escapeNameUnsafe($name) . " LIKE {$this->connect->escape($value)}";
        return $this;
    }

    public function orNotLike(string|array $name, mixed $value): static
    {
        $this->where .= " OR  " . Helper::escapeNameUnsafe($name) . " NOT LIKE {$this->connect->escape($value)}";
        return $this;
    }

    public function orIsNull(string|array $name): static
    {
        $this->where .= " OR  " . Helper::escapeNameUnsafe($name) . " IS NULL";
        return $this;
    }

    public function orIsNotNull(string|array $name): static
    {
        $this->where .= " OR  " . Helper::escapeNameUnsafe($name) . " IS NOT NULL";
        return $this;
    }

    public function orBetween(string|array $name, mixed $start, mixed $end): static
    {
        $this->where .= " OR  " . Helper::escapeNameUnsafe($name) . " BETWEEN {$this->connect->escape($start)} AND {$this->connect->escape($end)}";
        return $this;
    }

    public function orIn(string|array $name, array|Select $list): static
    {
        $this->where .= " OR  " . Helper::escapeNameUnsafe($name) . " IN {$this->escapeList($list)}";
        return $this;
    }

    public function orNotIn(string|array $name, array|Select $list): static
    {
        $this->where .= " OR  " . Helper::escapeNameUnsafe($name) . " NOT IN {$this->escapeList($list)}";
        return $this;
    }

    public function orExists(Select $select): static
    {
        $this->where .= " OR  EXISTS ({$select->query()})";
        return $this;
    }

    public function orNotExists(Select $select): static
    {
        $this->where .= " OR  NOT EXISTS ({$select->query()})";
        return $this;
    }

    public function orSubWhere(Where $where): static
    {
        $this->where .= " OR  ({$where->query()})";
        return $this;
    }

    public function orSubFn(Closure $sub): static
    {
        $this->where .= ' OR  (' . $sub(new Where($this->connect))->query() . ')';
        return $this;
    }
}