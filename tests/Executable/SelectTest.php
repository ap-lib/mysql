<?php declare(strict_types=1);

namespace AP\Mysql\Tests\Executable;

use AP\Mysql\Connect\ConnectDebug;
use AP\Mysql\Executable\Select;
use AP\Mysql\Raw;
use AP\Mysql\Statement\GroupBy;
use AP\Mysql\Statement\OrderBy;
use AP\Mysql\Statement\TableFactor;
use AP\Mysql\Statement\Where;
use PHPUnit\Framework\TestCase;

class SelectTest extends TestCase
{
    static public function s(
        string|TableFactor  $table,
        array               $expresions = [],
        Where|array|null    $where = null,
        Where|array|null    $having = null,
        GroupBy|string|null $group = null,
        OrderBy|string|null $order = null,
        ?int                $limit = null,
        ?int                $offset = null,
        bool                $distinct = false,
    ): Select
    {
        return new Select(
            connect: new ConnectDebug(),
            table: $table,
            columns: $expresions,
            where: $where,
            having: $having,
            group: $group,
            order: $order,
            limit: $limit,
            offset: $offset,
            distinct: $distinct,
        );
    }

    public function testMain(): void
    {
        $this->assertEquals(
            "SELECT * FROM `table`",
            self::s("table")->query()
        );

        $this->assertEquals(
            "SELECT `id`,`label` FROM `table`",
            self::s("table", ["id", "label"])->query()
        );

        $this->assertEquals(
            "SELECT * FROM `table` WHERE `id`=1",
            self::s("table", where: ["id" => 1])->query()
        );

        $this->assertEquals(
            "SELECT * FROM `table` WHERE `id`=1 AND `label`='hello'",
            self::s("table", where: ["id" => 1, "label" => "hello"])->query()
        );

        $s = self::s("table");
        $s->getWhereObject()->eq("id", 1);
        $this->assertEquals(
            "SELECT * FROM `table` WHERE `id`=1",
            $s->query()
        );

        $s = self::s("table");
        $s->getWhereObject()
            ->eq("id", 1)
            ->orEq("label", "hello");
        $this->assertEquals(
            "SELECT * FROM `table` WHERE `id`=1 OR  `label`='hello'",
            $s->query()
        );

        $this->assertEquals(
            "SELECT * FROM `table` HAVING `id`=1",
            self::s("table", having: ["id" => 1])->query()
        );

        $this->assertEquals(
            "SELECT * FROM `table` HAVING `id`=1 AND `label`='hello'",
            self::s("table", having: ["id" => 1, "label" => "hello"])->query()
        );

        $s = self::s("table");
        $s->getHavingObject()->eq("id", 1);
        $this->assertEquals(
            "SELECT * FROM `table` HAVING `id`=1",
            $s->query()
        );

        $s = self::s("table");
        $s->getHavingObject()
            ->eq("id", 1)
            ->orEq("label", "hello");
        $this->assertEquals(
            "SELECT * FROM `table` HAVING `id`=1 OR  `label`='hello'",
            $s->query()
        );


        $s = self::s("table");
        $s->orderBy()
            ->asc("id");
        $this->assertEquals(
            "SELECT * FROM `table` ORDER BY `id`",
            $s->query()
        );

        $s = self::s("table");
        $s->orderBy()
            ->desc("id");
        $this->assertEquals(
            "SELECT * FROM `table` ORDER BY `id` DESC",
            $s->query()
        );

        $this->assertEquals(
            "SELECT * FROM `table` LIMIT 10",
            self::s("table", limit: 10)->query()
        );

        $this->assertEquals(
            "SELECT * FROM `table` LIMIT 10,5",
            self::s("table", limit: 10, offset: 5)->query()
        );

        $this->assertEquals(
            "SELECT * FROM `table`",
            self::s("table", offset: 5)->query()
        );

        $this->assertEquals(
            "SELECT DISTINCT `name` FROM `table`",
            self::s("table", expresions: ["name"], distinct: true)->query()
        );

        $this->assertEquals(
            "SELECT DISTINCT `name` FROM `table`",
            self::s("table", expresions: ["name"], distinct: true)->query()
        );

        $this->assertEquals(
            "SELECT STRAIGHT_JOIN * FROM `table`",
            self::s("table")->straightJoin()->query()
        );

        $this->assertEquals(
            "SELECT SQL_SMALL_RESULT * FROM `table`",
            self::s("table")->sqlSmallResult()->query()
        );


        $this->assertEquals(
            "SELECT SQL_BIG_RESULT * FROM `table`",
            self::s("table")->sqlBigResult()->query()
        );
    }

    public function testMostPerformancesSimple(): void
    {
        $this->assertEquals(
            expected: "SELECT DISTINCT `id`,`name` AS `label` FROM `table` WHERE `group`=1 GROUP BY `id` ORDER BY `name` DESC LIMIT 10,20",
            actual: self::s(
                table: "table",
                expresions: [
                    "id",
                    "label" => "name"
                ],
                where: [
                    "group" => 1,
                ],
                having: [],
                group: "`id`",
                order: "`name` DESC",
                limit: 10,
                offset: 20,
                distinct: true
            )->query()
        );
    }

    public function testStepByStep(): void
    {
        // better performance then in one line, more clear and static typing code
        // in one line was removed, because it was so much magical code

        $unsafe_name1 = 'mine test';
        $unsafe_name2 = "Joe's test";

        $testCases = new Raw(
            "case when MOD(id,10) < 5 then %s when MOD(id,10) >= 5 then %s end",
            $unsafe_name1,
            $unsafe_name2
        );

        $connection = new ConnectDebug();

        $select = $connection->select(
            new TableFactor(
                table: "table",
                indexHintList: "IGNORE INDEX (product_id)"
            )
        )
            ->setColumns([
                "case"     => $testCases,
                "all"      => new Raw("count(*)"),
                "accepted" => new Raw("sum(if(accepted_to != 0, 1, 0))"),
            ])
            ->whereBetween("created_at", "2025-01-01 13:00:00", "2025-01-01 14:59:59")
            ->whereEq("product_id", 1);

        $select->groupByObject()->add("case");
        $select->orderBy()->asc("case");

        $this->assertEquals(
            "SELECT " .
            "case when MOD(id,10) < 5 then 'mine test' when MOD(id,10) >= 5 then 'Joe\'s test' end AS `case`" .
            ",count(*) AS `all`" .
            ",sum(if(accepted_to != 0, 1, 0)) AS `accepted`" .
            " FROM `table` IGNORE INDEX (product_id)" .
            " WHERE " .
            "`created_at` BETWEEN '2025-01-01 13:00:00' AND '2025-01-01 14:59:59'" .
            " AND `product_id`=1" .
            " ORDER BY `case`" .
            " ORDER BY `case`",
            $select->query()
        );
    }
}
