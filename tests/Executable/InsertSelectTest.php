<?php declare(strict_types=1);

namespace AP\Mysql\Tests\Executable;

use AP\Mysql\Connect\ConnectDebug;
use AP\Mysql\Executable\InsertSelect;
use AP\Mysql\Executable\Select;
use AP\Mysql\Raw;
use PHPUnit\Framework\TestCase;

class InsertSelectTest extends TestCase
{
    static public function i(
        string $table,
        Select $select,
        array  $cols = [],
        bool   $ignore = false,
        ?array $onDupKeyUpdate = null,
        string $partition = ""
    ): InsertSelect
    {
        return new InsertSelect(
            new ConnectDebug(),
            $table,
            $select,
            $cols,
            $ignore,
            $onDupKeyUpdate,
            $partition
        );
    }

    public function testMain(): void
    {
        $select1 = new Select(new ConnectDebug(), "source", ["id", "source_label"]);

        $this->assertEquals(
            "INSERT `table` SELECT `id`,`source_label` FROM `source`",
            self::i("table", $select1)->query()
        );

        $this->assertEquals(
            "INSERT `table`(`id`,`label`) SELECT `id`,`source_label` FROM `source`",
            self::i("table", $select1, ["id", "label"])->query()
        );

        $this->assertEquals(
            "INSERT `table`(`id`,`label`) SELECT `id`,`source_label` FROM `source`",
            self::i("table", $select1, ["id", "label"])->query()
        );

        $this->assertEquals(
            "INSERT IGNORE `table`(`id`,`label`) SELECT `id`,`source_label` FROM `source`",
            self::i("table", $select1, ["id", "label"])->setIgnore()->query()
        );

        $this->assertEquals(
            "INSERT IGNORE `table` PARTITION (prt1,prt2)(`id`,`label`) SELECT `id`,`source_label` FROM `source`",
            self::i("table", $select1, ["id", "label"])
                ->setPartition("prt1,prt2")
                ->setIgnore(true)
                ->query()
        );

        $this->assertEquals(
            "INSERT `table`(`id`,`label`) SELECT `id`,`source_label` FROM `source` ON DUPLICATE KEY UPDATE `e`=`e`+1,`le`=NOW()",
            self::i("table", $select1, ["id", "label"])
                ->setOnDupKeyUpdate([
                    "e"  => new Raw("`e`+%s", 1),
                    "le" => new Raw("NOW()")
                ])
                ->query()
        );
    }
}
