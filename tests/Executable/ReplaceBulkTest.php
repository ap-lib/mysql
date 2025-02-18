<?php declare(strict_types=1);

namespace AP\Mysql\Tests\Executable;

use AP\Mysql\Connect\ConnectDebug;
use AP\Mysql\Executable\InsertBulk;
use AP\Mysql\Executable\ReplaceBulk;
use AP\Mysql\Raw;
use PHPUnit\Framework\TestCase;

class ReplaceBulkTest extends TestCase
{
    static public function i(string $table, array $rows,): ReplaceBulk
    {
        return new ReplaceBulk(new ConnectDebug(), $table, $rows);
    }

    public function testBatch(): void
    {
        $rows = [
            ["id" => 1, "label" => "hello"],
            ["id" => 2, "label" => "world"],
            ["id" => 3, "label" => "privet"],
        ];

        $this->assertEquals(
            "REPLACE `table`(`id`,`label`) VALUE (1,'hello'),(2,'world'),(3,'privet')",
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
            "REPLACE `table`(`id`,`label`) VALUE (1,'hello'),(2,'world'),(3,'privet')",
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
                "REPLACE `table`(`id`,`label`) VALUE (1,'hello'),(2,'world')",
                "REPLACE `table`(`id`,`label`) VALUE (3,'privet')",
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
                "REPLACE `table` PARTITION (prt)(`id`,`label`) VALUE (1,'hello'),(2,'world')",
                "REPLACE `table` PARTITION (prt)(`id`,`label`) VALUE (3,'privet')",
            ]),
            implode(
                ";",
                iterator_to_array(
                    self::i("table", $rows)
                        ->setBatch(2)
                        ->setPartition("prt")
                        ->queries()
                )
            )
        );
    }

    public function testSimpleFormat(): void
    {
        $this->assertEquals(
            "REPLACE `table`(`label`) VALUE ('hello')",
            self::i(
                "table",
                [["label" => "hello"]]
            )
                ->queries()
                ->current()
        );

        $this->assertEquals(
            "REPLACE `table`(`label`) VALUE ('hel\'lo')",
            self::i(
                "table",
                [["label" => "hel'lo"]]
            )
                ->queries()
                ->current()
        );

        $this->assertEquals(
            "REPLACE `table`(`now_md5`) VALUE (MD5(NOW()))",
            self::i(
                "table",
                [["now_md5" => new Raw("MD5(NOW())")]]
            )
                ->queries()
                ->current()
        );

        $this->assertEquals(
            "REPLACE `table`(`int`) VALUE (1)",
            self::i(
                "table",
                [["int" => 1]]
            )
                ->queries()
                ->current()
        );
    }
}
