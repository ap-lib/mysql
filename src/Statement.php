<?php declare(strict_types=1);

namespace AP\Mysql;

interface Statement
{
    public function query(): string;
}