<?php declare(strict_types=1);

namespace AP\Mysql\Statement;

interface Statement
{
    public function query(): string;
}