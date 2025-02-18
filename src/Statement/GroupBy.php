<?php declare(strict_types=1);

namespace AP\Mysql\Statement;

use AP\Mysql\Connect\ConnectInterface;

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