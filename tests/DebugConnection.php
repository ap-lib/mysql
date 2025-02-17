<?php declare(strict_types=1);

namespace AP\Mysql\Tests;

use AP\Mysql\ConnectDebug;
use AP\Mysql\ConnectInterface;

class DebugConnection
{
    public static function escapeDebugMode(): ConnectInterface
    {
        return new ConnectDebug();
    }
}
