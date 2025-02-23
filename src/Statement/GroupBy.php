<?php declare(strict_types=1);

namespace AP\Mysql\Statement;

class GroupBy implements Statement
{
    private string $group = "";

    public function add(string|int $name): static
    {
        $this->group .= is_string($name) ? ",`$name`" : ",$name";
        return $this;
    }

    public function expr(string $expr): static
    {
        $this->group .= ",$expr";
        return $this;
    }

    public function query(): string
    {
        return substr($this->group, 1);
    }
}