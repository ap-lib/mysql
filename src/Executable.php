<?php declare(strict_types=1);

namespace AP\Mysql;

use mysqli_result;

interface Executable
{
    /**
     * @return mysqli_result|bool
     */
    public function exec(): mysqli_result|bool;
}