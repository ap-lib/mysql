<?php declare(strict_types=1);

namespace AP\Mysql\Tests\Executable;

use AP\Mysql\Connect\ConnectDebug;
use AP\Mysql\Executable\Insert;
use AP\Mysql\Raw;
use PHPUnit\Framework\TestCase;

class InsertTest extends TestCase
{
    static public function i(string $table, array $row,): Insert
    {
        return new Insert(new ConnectDebug(), $table, $row);
    }

    public function testMain(): void
    {
        $this->assertEquals(
            "INSERT `table`(`id`,`label`) VALUE (1,'hello')",
            self::i("table", ["id" => 1, "label" => "hello"])->query()
        );

        $this->assertEquals(
            "INSERT `table_arch`(`id`,`label`) VALUE (1,'hello')",
            self::i("table", ["id" => 1, "label" => "hello"])->setTable("table_arch")->query()
        );

        $this->assertEquals(
            "INSERT `table`(`id`,`label`) VALUE (2,'world')",
            self::i("table", ["id" => 1, "label" => "hello"])
                ->setRow(["id" => 2, "label" => "world"])
                ->query()
        );

        $this->assertEquals(
            "INSERT `table`(`now_md5`) VALUE (MD5(NOW()))",
            self::i("table", ["now_md5" => new Raw("MD5(NOW())")])->query()
        );

        $this->assertEquals(
            "INSERT IGNORE `table`(`id`,`label`) VALUE (1,'hello')",
            self::i("table", ["id" => 1, "label" => "hello"])
                ->setIgnore(true)
                ->query()
        );

        $this->assertEquals(
            "INSERT IGNORE `table` PARTITION (prt1,prt2)(`id`,`label`) VALUE (1,'hello')",
            self::i("table", ["id" => 1, "label" => "hello"])
                ->setPartition("prt1,prt2")
                ->setIgnore(true)
                ->query()
        );

        $this->assertEquals(
            "INSERT `table` PARTITION (prt1)(`id`,`label`) VALUE (1,'hello')",
            self::i("table", ["id" => 1, "label" => "hello"])
                ->setPartition("prt1")
                ->query()
        );

        $this->assertEquals(
            "INSERT `table`(`id`,`label`) VALUE (1,'hello') ON DUPLICATE KEY UPDATE `e`=`e`+1,`le`=NOW()",
            self::i("table", ["id" => 1, "label" => "hello"])
                ->setOnDupKeyUpdate([
                    "e"  => new Raw("`e`+%s", 1),
                    "le" => new Raw("NOW()")
                ])
                ->query()
        );
    }
}
