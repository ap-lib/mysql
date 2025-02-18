<?php declare(strict_types=1);

namespace AP\Mysql\Tests\Executable;

use AP\Mysql\Connect\ConnectDebug;
use AP\Mysql\Executable\InsertBulk;
use AP\Mysql\Raw;
use PHPUnit\Framework\TestCase;

class InsertBulkTest extends TestCase
{
    static public function i(string $table, array $rows,): InsertBulk
    {
        return new InsertBulk(new ConnectDebug(), $table, $rows);
    }

    public function testBatch(): void
    {
        $rows = [
            ["id" => 1, "label" => "hello"],
            ["id" => 2, "label" => "world"],
            ["id" => 3, "label" => "privet"],
        ];

        $this->assertEquals(
            "INSERT `table`(`id`,`label`) VALUE (1,'hello'),(2,'world'),(3,'privet')",
            implode(
                ";",
                iterator_to_array(
                    self::i("table", $rows)
                        ->setBatch(10)
                        ->queries()
                )
            )
        );

        $this->assertEquals(
            "INSERT `table`(`id`,`label`) VALUE (1,'hello'),(2,'world'),(3,'privet')",
            implode(
                ";",
                iterator_to_array(
                    self::i("table", $rows)
                        ->setBatch(3)
                        ->queries()
                )
            )
        );

        $this->assertEquals(
            implode(";", [
                "INSERT `table`(`id`,`label`) VALUE (1,'hello'),(2,'world')",
                "INSERT `table`(`id`,`label`) VALUE (3,'privet')",
            ]),
            implode(
                ";",
                iterator_to_array(
                    self::i("table", $rows)
                        ->setBatch(2)
                        ->queries()
                )
            )
        );

        $this->assertEquals(
            implode(";", [
                "INSERT IGNORE `table`(`id`,`label`) VALUE (1,'hello'),(2,'world')",
                "INSERT IGNORE `table`(`id`,`label`) VALUE (3,'privet')",
            ]),
            implode(
                ";",
                iterator_to_array(
                    self::i("table", $rows)
                        ->setBatch(2)
                        ->setIgnore()
                        ->queries()
                )
            )
        );

        $this->assertEquals(
            implode(";", [
                "INSERT IGNORE `table` PARTITION (prt)(`id`,`label`) VALUE (1,'hello'),(2,'world')",
                "INSERT IGNORE `table` PARTITION (prt)(`id`,`label`) VALUE (3,'privet')",
            ]),
            implode(
                ";",
                iterator_to_array(
                    self::i("table", $rows)
                        ->setBatch(2)
                        ->setIgnore()
                        ->setPartition("prt")
                        ->queries()
                )
            )
        );

        $this->assertEquals(
            implode(";", [
                "INSERT `table`(`id`,`label`) VALUE (1,'hello'),(2,'world') ON DUPLICATE KEY UPDATE `errors`=`errors`+1",
                "INSERT `table`(`id`,`label`) VALUE (3,'privet') ON DUPLICATE KEY UPDATE `errors`=`errors`+1",
            ]),
            implode(
                ";",
                iterator_to_array(
                    self::i("table", $rows)
                        ->setOnDupKeyUpdate([
                            "errors" => new Raw("`errors`+1")
                        ])
                        ->setBatch(2)
                        ->queries()
                )
            )
        );
    }

    public function testSimpleFormat(): void
    {
        $this->assertEquals(
            "INSERT `table`(`label`) VALUE ('hello')",
            self::i(
                "table",
                [["label" => "hello"]]
            )
                ->queries()
                ->current()
        );

        $this->assertEquals(
            "INSERT `table`(`label`) VALUE ('hel\'lo')",
            self::i(
                "table",
                [["label" => "hel'lo"]]
            )
                ->queries()
                ->current()
        );

        $this->assertEquals(
            "INSERT `table`(`now_md5`) VALUE (MD5(NOW()))",
            self::i(
                "table",
                [["now_md5" => new Raw("MD5(NOW())")]]
            )
                ->queries()
                ->current()
        );

        $this->assertEquals(
            "INSERT `table`(`int`) VALUE (1)",
            self::i(
                "table",
                [["int" => 1]]
            )
                ->queries()
                ->current()
        );
    }
}
