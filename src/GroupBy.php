<?php declare(strict_types=1);

namespace AP\Mysql;

class GroupBy
{
    public function __construct(protected ConnectInterface $connect)
    {
    }

    public function get(): string
    {
        return '';
    }
}