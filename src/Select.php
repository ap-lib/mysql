<?php declare(strict_types=1);

namespace AP\Mysql;

class Select implements Statement
{
    public function __construct(private ConnectInterface $connect)
    {
    }

    /**
     * @var string|array<string>|array<string,string>
     */
    protected array|string $from;

    public function query(): string
    {
        // TODO: Implement __toString() method.
        return '';
    }
}