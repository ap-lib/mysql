<?php declare(strict_types=1);

namespace AP\Mysql;

use Closure;

class Where implements Statement
{
    private string $where = '';

    /**
     * @param ConnectInterface $connect
     */
    public function __construct(
        private readonly ConnectInterface $connect,
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

    public function eq(string $name, mixed $value): static
    {
        $this->where .= " AND `$name`={$this->connect->escape($value)}";
        return $this;
    }

    public function notEq(string $name, mixed $value): static
    {
        $this->where .= " AND `$name`<>{$this->connect->escape($value)}";
        return $this;
    }

    public function gt(string $name, mixed $value): static
    {
        $this->where .= " AND `$name`>{$this->connect->escape($value)}";
        return $this;
    }

    public function lt(string $name, mixed $value): static
    {
        $this->where .= " AND `$name`<{$this->connect->escape($value)}";
        return $this;
    }

    public function gte(string $name, mixed $value): static
    {
        $this->where .= " AND `$name`>={$this->connect->escape($value)}";
        return $this;
    }

    public function lte(string $name, mixed $value): static
    {
        $this->where .= " AND `$name`<={$this->connect->escape($value)}";
        return $this;
    }

    public function like(string $name, mixed $value): static
    {
        $this->where .= " AND `$name` LIKE {$this->connect->escape($value)}";
        return $this;
    }

    public function notLike(string $name, mixed $value): static
    {
        $this->where .= " AND `$name` NOT LIKE {$this->connect->escape($value)}";
        return $this;
    }

    public function isNull(string $name): static
    {
        $this->where .= " AND `$name` IS NULL";
        return $this;
    }

    public function isNotNull(string $name): static
    {
        $this->where .= " AND `$name` IS NOT NULL";
        return $this;
    }

    public function between(string $name, mixed $start, mixed $end): static
    {
        $this->where .= " AND `$name` BETWEEN {$this->connect->escape($start)} AND {$this->connect->escape($end)}";
        return $this;
    }

    public function in(string $name, array|Select $list): static
    {
        $this->where .= " AND `$name` IN {$this->escapeList($list)}";
        return $this;
    }

    public function notIn(string $name, array|Select $list): static
    {
        $this->where .= " AND `$name` NOT IN {$this->escapeList($list)}";
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

    public function orEq(string $name, mixed $value): static
    {
        $this->where .= " OR  `$name`={$this->connect->escape($value)}";
        return $this;
    }

    public function orNotEq(string $name, mixed $value): static
    {
        $this->where .= " OR  `$name`<>{$this->connect->escape($value)}";
        return $this;
    }

    public function orGt(string $name, mixed $value): static
    {
        $this->where .= " OR  `$name`>{$this->connect->escape($value)}";
        return $this;
    }

    public function orLt(string $name, mixed $value): static
    {
        $this->where .= " OR  `$name`<{$this->connect->escape($value)}";
        return $this;
    }

    public function orGte(string $name, mixed $value): static
    {
        $this->where .= " OR  `$name`>={$this->connect->escape($value)}";
        return $this;
    }

    public function orLte(string $name, mixed $value): static
    {
        $this->where .= " OR  `$name`<={$this->connect->escape($value)}";
        return $this;
    }

    public function orLike(string $name, mixed $value): static
    {
        $this->where .= " OR  `$name` LIKE {$this->connect->escape($value)}";
        return $this;
    }

    public function orNotLike(string $name, mixed $value): static
    {
        $this->where .= " OR  `$name` NOT LIKE {$this->connect->escape($value)}";
        return $this;
    }

    public function orIsNull(string $name): static
    {
        $this->where .= " OR  `$name` IS NULL";
        return $this;
    }

    public function orIsNotNull(string $name): static
    {
        $this->where .= " OR  `$name` IS NOT NULL";
        return $this;
    }

    public function orBetween(string $name, mixed $start, mixed $end): static
    {
        $this->where .= " OR  `$name` BETWEEN {$this->connect->escape($start)} AND {$this->connect->escape($end)}";
        return $this;
    }

    public function orIn(string $name, array|Select $list): static
    {
        $this->where .= " OR  `$name` IN {$this->escapeList($list)}";
        return $this;
    }

    public function orNotIn(string $name, array|Select $list): static
    {
        $this->where .= " OR  `$name` NOT IN {$this->escapeList($list)}";
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