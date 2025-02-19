<?php declare(strict_types=1);

namespace AP\Mysql\Statement;

use AP\Mysql\Executable\Select;
use AP\Mysql\Raw;

class Joins implements Statement
{
    private string $joins = "";

    public function innerJoin(
        string|Select    $table,
        string|Raw|Where $specification,
        ?string          $alias,
        bool             $straightJoin = false,
        string           $partition = "",
        string           $indexHintList = "",
    ): static
    {
        $this->joins .= " INNER JOIN `$table`";
        return $this;
    }

    public function leftJoin(string|int $name): static
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