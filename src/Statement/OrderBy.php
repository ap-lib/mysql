<?php declare(strict_types=1);

namespace AP\Mysql\Statement;

class OrderBy implements Statement
{
    private string $order = "";

    public function asc(string|int $name): static
    {
        $this->order .= is_string($name) ? ",`$name`" : ",$name";
        return $this;
    }

    public function desc(string|int $name): static
    {
        $this->order .= is_string($name) ? ",`$name` DESC" : ",$name DESC";
        return $this;
    }

    public function exprAsc(string $expr): static
    {
        $this->order .= ",$expr";
        return $this;
    }

    public function exprDesc(string $expr): static
    {
        $this->order .= ",$expr DESC";
        return $this;
    }

    public function query(): string
    {
        return substr($this->order, 1);
    }
}