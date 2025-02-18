<?php declare(strict_types=1);

namespace AP\Mysql\Tests\Executable;

use AP\Mysql\Connect\ConnectDebug;
use AP\Mysql\Executable\Insert;
use AP\Mysql\Executable\Replace;
use AP\Mysql\Raw;
use PHPUnit\Framework\TestCase;

class ReplaceTest extends TestCase
{
    static public function i(string $table, array $row,): Replace
    {
        return new Replace(new ConnectDebug(), $table, $row);
    }

    public function testMain(): void
    {
        $this->assertEquals(
            "REPLACE `table`(`id`,`label`) VALUE (1,'hello')",
            self::i("table", ["id" => 1, "label" => "hello"])->query()
        );

        $this->assertEquals(
            "REPLACE `table`(`now_md5`) VALUE (MD5(NOW()))",
            self::i("table", ["now_md5" => new Raw("MD5(NOW())")])->query()
        );

        $this->assertEquals(
            "REPLACE `table` PARTITION (prt1)(`id`,`label`) VALUE (1,'hello')",
            self::i("table", ["id" => 1, "label" => "hello"])
                ->setPartition("prt1")
                ->query()
        );
    }
}
