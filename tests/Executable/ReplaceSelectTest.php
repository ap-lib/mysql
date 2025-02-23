<?php declare(strict_types=1);

namespace AP\Mysql\Tests\Executable;

use AP\Mysql\Connect\ConnectDebug;
use AP\Mysql\Executable\InsertSelect;
use AP\Mysql\Executable\ReplaceSelect;
use AP\Mysql\Executable\Select;
use AP\Mysql\Raw;
use PHPUnit\Framework\TestCase;

class ReplaceSelectTest extends TestCase
{
    static public function i(
        string $table,
        Select $select,
        array  $cols,
        string $partition = ""
    ): ReplaceSelect
    {
        return new ReplaceSelect(
            new ConnectDebug(),
            $table,
            $select,
            $cols,
            $partition
        );
    }

    public function testMain(): void
    {
        $select1 = (new Select(new ConnectDebug(), "source", ["id", "source_label"]));

        $this->assertEquals(
            "REPLACE `table`(`id`,`label`) {$select1->query()}",
            self::i("table", $select1, ["id", "label"])->query()
        );

        $this->assertEquals(
            "REPLACE `table`(`id`,`label`) {$select1->query()}",
            self::i("table", $select1, ["id", "label"])->query()
        );

        $this->assertEquals(
            "REPLACE `table` PARTITION (prt1,prt2)(`id`,`label`) {$select1->query()}",
            self::i("table", $select1, ["id", "label"])
                ->setPartition("prt1,prt2")
                ->query()
        );

    }
}
