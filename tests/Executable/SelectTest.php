<?php declare(strict_types=1);

namespace AP\Mysql\Tests\Executable;

use AP\Mysql\Connect\ConnectDebug;
use AP\Mysql\Executable\Select;
use AP\Mysql\Raw;
use AP\Mysql\Statement\TableFactor;
use AP\Mysql\Statement\Where;
use PHPUnit\Framework\TestCase;


class SelectTest extends TestCase
{
    static public function s(string|TableFactor $table, array $columns = []): Select
    {
        return new Select(
            connect: new ConnectDebug(),
            table: $table,
            columns: $columns,
        );
    }

    static public function w(): Where
    {
        return new Where(new ConnectDebug());
    }

    public function testMain(): void
    {
        $this->assertEquals(/** @lang text */ "SELECT * FROM `table`", self::s("table")->query());
        $this->assertEquals(/** @lang text */ "SELECT `id`,`label` FROM `table`", self::s("table", ["id", "label"])->query());
        $this->assertEquals(/** @lang text */ "SELECT * FROM `table` WHERE `id`=1", self::s("table")->whereEq("id", 1)->query());
        $this->assertEquals(/** @lang text */ "SELECT * FROM `table` WHERE `id`=1 OR  `label`='hello'", self::s("table")->whereEq("id", 1)->orWhereEq("label", "hello")->query());

        $this->assertEquals(/** @lang text */ "SELECT * FROM `table` HAVING `id`=1", self::s("table")->havingEq("id", 1)->query());
        $this->assertEquals(/** @lang text */ "SELECT * FROM `table` HAVING `id`=1 OR  `label`='hello'", self::s("table")->havingEq("id", 1)->orHavingEq("label", "hello")->query());

        $this->assertEquals(/** @lang text */ "SELECT * FROM `table` LIMIT 10", self::s("table")->setLimit(10)->query());
        $this->assertEquals(/** @lang text */ "SELECT * FROM `table` LIMIT 5,10", self::s("table")->setLimit(10, 5)->query());
        $this->assertEquals(/** @lang text */ "SELECT * FROM `table` LIMIT 5,10", self::s("table")->setLimit(10)->setOffset(5)->query());

        $this->assertEquals(/** @lang text */ "SELECT DISTINCT `name` FROM `table`", self::s("table", columns: ["name"])->distinct()->query());

        $this->assertEquals(/** @lang text */ "SELECT STRAIGHT_JOIN * FROM `table`", self::s("table")->straightJoin()->query());

        $this->assertEquals(/** @lang text */ "SELECT SQL_SMALL_RESULT * FROM `table`", self::s("table")->sqlSmallResult()->query());
        $this->assertEquals(/** @lang text */ "SELECT SQL_BIG_RESULT * FROM `table`", self::s("table")->sqlBigResult()->query());
        $this->assertEquals(/** @lang text */ "SELECT * FROM `table`", self::s("table")->sqlBigResult()->sqlDefResult()->query());

        $this->assertEquals(/** @lang text */ "SELECT * FROM `backup`", self::s("main")->setTable('backup')->query());
        $this->assertEquals(/** @lang text */ "SELECT * FROM `backup`", self::s("main")->setTable('backup')->query());

        $this->assertEquals(
        /** @lang text */ "SELECT ROW_NUMBER() OVER w AS `row_number` FROM `table` WINDOW w AS (ORDER BY val)",
            self::s("table", ["row_number" => new Raw("ROW_NUMBER() OVER w")])
                ->setWindow("w AS (ORDER BY val)")
                ->query()
        );
    }

    public function testJoin(): void
    {
        $this->assertEquals(
        /** @lang text */ "SELECT * FROM `table` JOIN `user` ON user.id=table.user_id",
            self::s("table")->joinInner("user", "user.id=table.user_id")->query()
        );

        $this->assertEquals(
        /** @lang text */ "SELECT * FROM `table` LEFT JOIN `user` ON user.id=table.user_id",
            self::s("table")->joinLeft("user", "user.id=table.user_id")->query()
        );

        $this->assertEquals(
        /** @lang text */ "SELECT * FROM `table` LEFT OUTER JOIN `user` ON user.id=table.user_id",
            self::s("table")->joinLeft("user", "user.id=table.user_id", true)->query()
        );

        $this->assertEquals(
        /** @lang text */ "SELECT * FROM `table` RIGHT JOIN `user` ON user.id=table.user_id",
            self::s("table")->joinRight("user", "user.id=table.user_id")->query()
        );

        $this->assertEquals(
        /** @lang text */ "SELECT * FROM `table` RIGHT OUTER JOIN `user` ON user.id=table.user_id",
            self::s("table")->joinRight("user", "user.id=table.user_id", true)->query()
        );

        $this->assertEquals(
        /** @lang text */ "SELECT * FROM `table` STRAIGHT_JOIN `user` ON user.id=table.user_id",
            self::s("table")->joinStraight("user", "user.id=table.user_id")->query()
        );
    }

    public function testSummaryForABTest(): void
    {
        $unsafe_name1 = 'mine test';
        $unsafe_name2 = "Joe's test";

        $this->assertEquals(
            "SELECT " .
            "case when MOD(id,10) < 5 then 'mine test' when MOD(id,10) >= 5 then 'Joe\'s test' end AS `case`" .
            ",count(*) AS `all`" .
            ",sum(if(accepted_to != 0, 1, 0)) AS `accepted`" .
            " FROM `table` IGNORE INDEX (product_id)" .
            " WHERE " .
            "`created_at` BETWEEN '2025-01-01 13:00:00' AND '2025-01-01 14:59:59'" .
            " AND `product_id`=1" .
            " GROUP BY `case`" .
            " ORDER BY `case`",
            (new ConnectDebug())->select(
                new TableFactor(
                    table: "table",
                    indexHintList: "IGNORE INDEX (product_id)"
                )
            )
                ->setColumns([
                    "case"     => new Raw(
                        "case when MOD(id,10) < 5 then %s when MOD(id,10) >= 5 then %s end",
                        $unsafe_name1,
                        $unsafe_name2
                    ),
                    "all"      => new Raw("count(*)"),
                    "accepted" => new Raw("sum(if(accepted_to != 0, 1, 0))"),
                ])
                ->whereBetween("created_at", "2025-01-01 13:00:00", "2025-01-01 14:59:59")
                ->whereEq("product_id", 1)
                ->group("case")
                ->order("case")
                ->query()
        );
    }

    public function testWhere(): void
    {
        $subSelect     = (new Select(new ConnectDebug(), "subtable", ["id"]))
            ->whereEq("name", "Yuri");
        $subSelectText = $subSelect->query();

        $ds = /** @lang text */
            "SELECT * FROM `tbl` WHERE";

        $this->assertEquals("$ds (foo='boo' or foo in null)", self::s("tbl")->whereCond("foo=%s or foo in null", "boo")->query());
        $this->assertEquals("$ds (foo=1 or foo in null)", self::s("tbl")->whereCond("foo=%s or foo in null", 1)->query());
        $this->assertEquals("$ds `foo`='boo'", self::s("tbl")->whereEq("foo", "boo")->query());
        $this->assertEquals("$ds `foo`<>'boo'", self::s("tbl")->whereNotEq("foo", "boo")->query());
        $this->assertEquals("$ds `foo`>1", self::s("tbl")->whereGt("foo", 1)->query());
        $this->assertEquals("$ds `foo`<1", self::s("tbl")->whereLt("foo", 1)->query());
        $this->assertEquals("$ds `foo`>=1", self::s("tbl")->whereGte("foo", 1)->query());
        $this->assertEquals("$ds `foo`<=1", self::s("tbl")->whereLte("foo", 1)->query());
        $this->assertEquals("$ds `foo` LIKE '%boo'", self::s("tbl")->whereLike("foo", "%boo")->query());
        $this->assertEquals("$ds `foo` NOT LIKE 'boo%'", self::s("tbl")->whereNotLike("foo", "boo%")->query());
        $this->assertEquals("$ds `foo` IS NULL", self::s("tbl")->whereIsNull("foo")->query());
        $this->assertEquals("$ds `foo` IS NOT NULL", self::s("tbl")->whereIsNotNull("foo")->query());
        $this->assertEquals("$ds `foo` BETWEEN 1 AND 2", self::s("tbl")->whereBetween("foo", 1, 2)->query());
        $this->assertEquals("$ds `foo` IN (1,2)", self::s("tbl")->whereIn("foo", [1, 2])->query());
        $this->assertEquals("$ds `foo` NOT IN (1,2)", self::s("tbl")->whereNotIn("foo", [1, 2])->query());
        $this->assertEquals("$ds (`foo`=1)", self::s("tbl")->whereSubWhere(self::w()->eq("foo", 1))->query());
        $this->assertEquals("$ds (`foo`=1)", self::s("tbl")->whereSubFn(fn(Where $sub) => $sub->eq("foo", 1))->query());
        $this->assertEquals("$ds EXISTS ($subSelectText)", self::s("tbl")->whereExists($subSelect)->query());
        $this->assertEquals("$ds NOT EXISTS ($subSelectText)", self::s("tbl")->whereNotExists($subSelect)->query());
        $this->assertEquals("$ds `foo` IN ($subSelectText)", self::s("tbl")->whereIn("foo", $subSelect)->query());
        $this->assertEquals("$ds `foo` NOT IN ($subSelectText)", self::s("tbl")->whereNotIn("foo", $subSelect)->query());


        $this->assertEquals("$ds (foo='boo' or foo in null)", self::s("tbl")->orWhereCond("foo=%s or foo in null", "boo")->query());
        $this->assertEquals("$ds (foo=1 or foo in null)", self::s("tbl")->orWhereCond("foo=%s or foo in null", 1)->query());
        $this->assertEquals("$ds `foo`='boo'", self::s("tbl")->orWhereEq("foo", "boo")->query());
        $this->assertEquals("$ds `foo`<>'boo'", self::s("tbl")->orWhereNotEq("foo", "boo")->query());
        $this->assertEquals("$ds `foo`>1", self::s("tbl")->orWhereGt("foo", 1)->query());
        $this->assertEquals("$ds `foo`<1", self::s("tbl")->orWhereLt("foo", 1)->query());
        $this->assertEquals("$ds `foo`>=1", self::s("tbl")->orWhereGte("foo", 1)->query());
        $this->assertEquals("$ds `foo`<=1", self::s("tbl")->orWhereLte("foo", 1)->query());
        $this->assertEquals("$ds `foo` LIKE '%boo'", self::s("tbl")->orWhereLike("foo", "%boo")->query());
        $this->assertEquals("$ds `foo` NOT LIKE 'boo%'", self::s("tbl")->orWhereNotLike("foo", "boo%")->query());
        $this->assertEquals("$ds `foo` IS NULL", self::s("tbl")->orWhereIsNull("foo")->query());
        $this->assertEquals("$ds `foo` IS NOT NULL", self::s("tbl")->orWhereIsNotNull("foo")->query());
        $this->assertEquals("$ds `foo` BETWEEN 1 AND 2", self::s("tbl")->orWhereBetween("foo", 1, 2)->query());
        $this->assertEquals("$ds `foo` IN (1,2)", self::s("tbl")->orWhereIn("foo", [1, 2])->query());
        $this->assertEquals("$ds `foo` NOT IN (1,2)", self::s("tbl")->orWhereNotIn("foo", [1, 2])->query());
        $this->assertEquals("$ds (`foo`=1)", self::s("tbl")->orWhereSubWhere(self::w()->eq("foo", 1))->query());
        $this->assertEquals("$ds (`foo`=1)", self::s("tbl")->orWhereSubFn(fn(Where $sub) => $sub->eq("foo", 1))->query());
        $this->assertEquals("$ds EXISTS ($subSelectText)", self::s("tbl")->orWhereExists($subSelect)->query());
        $this->assertEquals("$ds NOT EXISTS ($subSelectText)", self::s("tbl")->orWhereNotExists($subSelect)->query());
        $this->assertEquals("$ds `foo` IN ($subSelectText)", self::s("tbl")->orWhereIn("foo", $subSelect)->query());
        $this->assertEquals("$ds `foo` NOT IN ($subSelectText)", self::s("tbl")->orWhereNotIn("foo", $subSelect)->query());
    }

    public function testHaving(): void
    {
        $subSelect     = (new Select(new ConnectDebug(), "subtable", ["id"]))
            ->whereEq("name", "Yuri");
        $subSelectText = $subSelect->query();

        $ds = /** @lang text */
            "SELECT * FROM `tbl` HAVING";

        $this->assertEquals("$ds (foo='boo' or foo in null)", self::s("tbl")->havingCond("foo=%s or foo in null", "boo")->query());
        $this->assertEquals("$ds (foo=1 or foo in null)", self::s("tbl")->havingCond("foo=%s or foo in null", 1)->query());
        $this->assertEquals("$ds `foo`='boo'", self::s("tbl")->havingEq("foo", "boo")->query());
        $this->assertEquals("$ds `foo`<>'boo'", self::s("tbl")->havingNotEq("foo", "boo")->query());
        $this->assertEquals("$ds `foo`>1", self::s("tbl")->havingGt("foo", 1)->query());
        $this->assertEquals("$ds `foo`<1", self::s("tbl")->havingLt("foo", 1)->query());
        $this->assertEquals("$ds `foo`>=1", self::s("tbl")->havingGte("foo", 1)->query());
        $this->assertEquals("$ds `foo`<=1", self::s("tbl")->havingLte("foo", 1)->query());
        $this->assertEquals("$ds `foo` LIKE '%boo'", self::s("tbl")->havingLike("foo", "%boo")->query());
        $this->assertEquals("$ds `foo` NOT LIKE 'boo%'", self::s("tbl")->havingNotLike("foo", "boo%")->query());
        $this->assertEquals("$ds `foo` IS NULL", self::s("tbl")->havingIsNull("foo")->query());
        $this->assertEquals("$ds `foo` IS NOT NULL", self::s("tbl")->havingIsNotNull("foo")->query());
        $this->assertEquals("$ds `foo` BETWEEN 1 AND 2", self::s("tbl")->havingBetween("foo", 1, 2)->query());
        $this->assertEquals("$ds `foo` IN (1,2)", self::s("tbl")->havingIn("foo", [1, 2])->query());
        $this->assertEquals("$ds `foo` NOT IN (1,2)", self::s("tbl")->havingNotIn("foo", [1, 2])->query());
        $this->assertEquals("$ds (`foo`=1)", self::s("tbl")->havingSubWhere(self::w()->eq("foo", 1))->query());
        $this->assertEquals("$ds (`foo`=1)", self::s("tbl")->havingSubFn(fn(Where $sub) => $sub->eq("foo", 1))->query());
        $this->assertEquals("$ds EXISTS ($subSelectText)", self::s("tbl")->havingExists($subSelect)->query());
        $this->assertEquals("$ds NOT EXISTS ($subSelectText)", self::s("tbl")->havingNotExists($subSelect)->query());
        $this->assertEquals("$ds `foo` IN ($subSelectText)", self::s("tbl")->havingIn("foo", $subSelect)->query());
        $this->assertEquals("$ds `foo` NOT IN ($subSelectText)", self::s("tbl")->havingNotIn("foo", $subSelect)->query());


        $this->assertEquals("$ds (foo='boo' or foo in null)", self::s("tbl")->orHavingCond("foo=%s or foo in null", "boo")->query());
        $this->assertEquals("$ds (foo=1 or foo in null)", self::s("tbl")->orHavingCond("foo=%s or foo in null", 1)->query());
        $this->assertEquals("$ds `foo`='boo'", self::s("tbl")->orHavingEq("foo", "boo")->query());
        $this->assertEquals("$ds `foo`<>'boo'", self::s("tbl")->orHavingNotEq("foo", "boo")->query());
        $this->assertEquals("$ds `foo`>1", self::s("tbl")->orHavingGt("foo", 1)->query());
        $this->assertEquals("$ds `foo`<1", self::s("tbl")->orHavingLt("foo", 1)->query());
        $this->assertEquals("$ds `foo`>=1", self::s("tbl")->orHavingGte("foo", 1)->query());
        $this->assertEquals("$ds `foo`<=1", self::s("tbl")->orHavingLte("foo", 1)->query());
        $this->assertEquals("$ds `foo` LIKE '%boo'", self::s("tbl")->orHavingLike("foo", "%boo")->query());
        $this->assertEquals("$ds `foo` NOT LIKE 'boo%'", self::s("tbl")->orHavingNotLike("foo", "boo%")->query());
        $this->assertEquals("$ds `foo` IS NULL", self::s("tbl")->orHavingIsNull("foo")->query());
        $this->assertEquals("$ds `foo` IS NOT NULL", self::s("tbl")->orHavingIsNotNull("foo")->query());
        $this->assertEquals("$ds `foo` BETWEEN 1 AND 2", self::s("tbl")->orHavingBetween("foo", 1, 2)->query());
        $this->assertEquals("$ds `foo` IN (1,2)", self::s("tbl")->orHavingIn("foo", [1, 2])->query());
        $this->assertEquals("$ds `foo` NOT IN (1,2)", self::s("tbl")->orHavingNotIn("foo", [1, 2])->query());
        $this->assertEquals("$ds (`foo`=1)", self::s("tbl")->orHavingSubWhere(self::w()->eq("foo", 1))->query());
        $this->assertEquals("$ds (`foo`=1)", self::s("tbl")->orHavingSubFn(fn(Where $sub) => $sub->eq("foo", 1))->query());
        $this->assertEquals("$ds EXISTS ($subSelectText)", self::s("tbl")->orHavingExists($subSelect)->query());
        $this->assertEquals("$ds NOT EXISTS ($subSelectText)", self::s("tbl")->orHavingNotExists($subSelect)->query());
        $this->assertEquals("$ds `foo` IN ($subSelectText)", self::s("tbl")->orHavingIn("foo", $subSelect)->query());
        $this->assertEquals("$ds `foo` NOT IN ($subSelectText)", self::s("tbl")->orHavingNotIn("foo", $subSelect)->query());
    }

    public function testOrdering(): void
    {
        $ds = /** @lang text */
            "SELECT * FROM `table` ORDER BY";

        $this->assertEquals("$ds `name`", self::s("table")->order("name")->query());
        $this->assertEquals("$ds `id`,`name`", self::s("table")->order("id")->order("name")->query());
        $this->assertEquals("$ds `age` DESC", self::s("table")->orderDesc("age")->query());
        $this->assertEquals("$ds LENGTH(name)", self::s("table")->orderExpr("LENGTH(name)")->query());
        $this->assertEquals("$ds LENGTH(name) DESC", self::s("table")->orderExprDesc("LENGTH(name)")->query());
    }

    public function testGrouping(): void
    {
        $ds = /** @lang text */
            "SELECT * FROM `table` GROUP BY";

        $this->assertEquals("$ds `name`", self::s("table")->group("name")->query());
        $this->assertEquals("$ds `id`,`name`", self::s("table")->group("id")->group("name")->query());
        $this->assertEquals("$ds LENGTH(name)", self::s("table")->groupExpr("LENGTH(name)")->query());
    }
}
