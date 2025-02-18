<?php declare(strict_types=1);

namespace AP\Mysql;

use AP\Mysql\Connect\ConnectInterface;

readonly class Raw
{
    public array $params;

    /**
     * @param string $expression no use any user input here,
     * @param ...$params mixed for all unsafe data from user input or data from database/cache use params
     */
    public function __construct(
        public string $expression,
                      ...$params
    )
    {
        $this->params = $params;
    }

    protected function queryWithParams(ConnectInterface $connect)
    {
        $params = $this->params;
        foreach ($params as $k => &$v) {
            $params[$k] = $connect->escape($v);
        }
        return sprintf($this->expression, ...$params);
    }

    public function escape(ConnectInterface $connect): string
    {
        return empty($this->params)
            ? $this->expression
            : $this->queryWithParams($connect);
    }
}