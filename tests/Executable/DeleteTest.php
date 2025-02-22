<?php declare(strict_types=1);

namespace AP\Mysql\Tests\Executable;

use AP\Mysql\Connect\ConnectDebug;
use AP\Mysql\Executable\Delete;
use AP\Mysql\Executable\Select;
use AP\Mysql\Statement\OrderBy;
use AP\Mysql\Statement\Where;
use PHPUnit\Framework\TestCase;

class DeleteTest extends TestCase
{
    static public function d(
        string              $table,
        Where|array|null    $where = null,
        OrderBy|string|null $order = null,
        ?int                $limit = null,
    ): Delete
    {
        return new Delete(
            new ConnectDebug(),
            $table,
            $where,
            $order,
            $limit
        );
    }

    static public function w(): Where
    {
        return new Where(new ConnectDebug());
    }

    public function testBasicSelect(): void
    {
        $ds = "DELETE FROM `table`";

        $this->assertEquals("$ds", self::d("table")->query());
        $this->assertEquals("$ds PARTITION (p1, p2)", self::d("table")->setPartitions("p1, p2")->query());
        $this->assertEquals("$ds AS `t`", self::d("table")->setTableAlias("t")->query());
        $this->assertEquals("$ds AS `t` PARTITION (p1, p2)", self::d("table")->setTableAlias("t")->setPartitions("p1, p2")->query());
        $this->assertEquals("$ds LIMIT 10", self::d("table")->setLimit(10)->query());
        $this->assertEquals("$ds LIMIT 0", self::d("table")->setLimit(0)->query());

        // option with error, be performance reason it works with no validation
        $this->assertEquals("$ds LIMIT -10", self::d("table")->setLimit(-10)->query());
    }

    public function testWhereMapping(): void
    {
        $subSelect     = new Select(new ConnectDebug(), "subtable", ["id"], ["name" => "Yuri"]);
        $subSelectText = $subSelect->query();

        $ds = "DELETE FROM `tbl` WHERE";

        $this->assertEquals("$ds (foo='boo' or foo in null)", self::d("tbl")->whereCond("foo=%s or foo in null", "boo")->query());
        $this->assertEquals("$ds (foo=1 or foo in null)", self::d("tbl")->whereCond("foo=%s or foo in null", 1)->query());
        $this->assertEquals("$ds `foo`='boo'", self::d("tbl")->whereEq("foo", "boo")->query());
        $this->assertEquals("$ds `foo`<>'boo'", self::d("tbl")->whereNotEq("foo", "boo")->query());
        $this->assertEquals("$ds `foo`>1", self::d("tbl")->whereGt("foo", 1)->query());
        $this->assertEquals("$ds `foo`<1", self::d("tbl")->whereLt("foo", 1)->query());
        $this->assertEquals("$ds `foo`>=1", self::d("tbl")->whereGte("foo", 1)->query());
        $this->assertEquals("$ds `foo`<=1", self::d("tbl")->whereLte("foo", 1)->query());
        $this->assertEquals("$ds `foo` LIKE '%boo'", self::d("tbl")->whereLike("foo", "%boo")->query());
        $this->assertEquals("$ds `foo` NOT LIKE 'boo%'", self::d("tbl")->whereNotLike("foo", "boo%")->query());
        $this->assertEquals("$ds `foo` IS NULL", self::d("tbl")->whereIsNull("foo")->query());
        $this->assertEquals("$ds `foo` IS NOT NULL", self::d("tbl")->whereIsNotNull("foo")->query());
        $this->assertEquals("$ds `foo` BETWEEN 1 AND 2", self::d("tbl")->whereBetween("foo", 1, 2)->query());
        $this->assertEquals("$ds `foo` IN (1,2)", self::d("tbl")->whereIn("foo", [1, 2])->query());
        $this->assertEquals("$ds `foo` NOT IN (1,2)", self::d("tbl")->whereNotIn("foo", [1, 2])->query());
        $this->assertEquals("$ds (`foo`=1)", self::d("tbl")->whereSubWhere(self::w()->eq("foo", 1))->query());
        $this->assertEquals("$ds (`foo`=1)", self::d("tbl")->whereSubFn(fn(Where $sub) => $sub->eq("foo", 1))->query());
        $this->assertEquals("$ds EXISTS ($subSelectText)", self::d("tbl")->whereExists($subSelect)->query());
        $this->assertEquals("$ds NOT EXISTS ($subSelectText)", self::d("tbl")->whereNotExists($subSelect)->query());
        $this->assertEquals("$ds `foo` IN ($subSelectText)", self::d("tbl")->whereIn("foo", $subSelect)->query());
        $this->assertEquals("$ds `foo` NOT IN ($subSelectText)", self::d("tbl")->whereNotIn("foo", $subSelect)->query());


        $this->assertEquals("$ds (foo='boo' or foo in null)", self::d("tbl")->orWhereCond("foo=%s or foo in null", "boo")->query());
        $this->assertEquals("$ds (foo=1 or foo in null)", self::d("tbl")->orWhereCond("foo=%s or foo in null", 1)->query());
        $this->assertEquals("$ds `foo`='boo'", self::d("tbl")->orWhereEq("foo", "boo")->query());
        $this->assertEquals("$ds `foo`<>'boo'", self::d("tbl")->orWhereNotEq("foo", "boo")->query());
        $this->assertEquals("$ds `foo`>1", self::d("tbl")->orWhereGt("foo", 1)->query());
        $this->assertEquals("$ds `foo`<1", self::d("tbl")->orWhereLt("foo", 1)->query());
        $this->assertEquals("$ds `foo`>=1", self::d("tbl")->orWhereGte("foo", 1)->query());
        $this->assertEquals("$ds `foo`<=1", self::d("tbl")->orWhereLte("foo", 1)->query());
        $this->assertEquals("$ds `foo` LIKE '%boo'", self::d("tbl")->orWhereLike("foo", "%boo")->query());
        $this->assertEquals("$ds `foo` NOT LIKE 'boo%'", self::d("tbl")->orWhereNotLike("foo", "boo%")->query());
        $this->assertEquals("$ds `foo` IS NULL", self::d("tbl")->orWhereIsNull("foo")->query());
        $this->assertEquals("$ds `foo` IS NOT NULL", self::d("tbl")->orWhereIsNotNull("foo")->query());
        $this->assertEquals("$ds `foo` BETWEEN 1 AND 2", self::d("tbl")->orWhereBetween("foo", 1, 2)->query());
        $this->assertEquals("$ds `foo` IN (1,2)", self::d("tbl")->orWhereIn("foo", [1, 2])->query());
        $this->assertEquals("$ds `foo` NOT IN (1,2)", self::d("tbl")->orWhereNotIn("foo", [1, 2])->query());
        $this->assertEquals("$ds (`foo`=1)", self::d("tbl")->orWhereSubWhere(self::w()->eq("foo", 1))->query());
        $this->assertEquals("$ds (`foo`=1)", self::d("tbl")->orWhereSubFn(fn(Where $sub) => $sub->eq("foo", 1))->query());
        $this->assertEquals("$ds EXISTS ($subSelectText)", self::d("tbl")->orWhereExists($subSelect)->query());
        $this->assertEquals("$ds NOT EXISTS ($subSelectText)", self::d("tbl")->orWhereNotExists($subSelect)->query());
        $this->assertEquals("$ds `foo` IN ($subSelectText)", self::d("tbl")->orWhereIn("foo", $subSelect)->query());
        $this->assertEquals("$ds `foo` NOT IN ($subSelectText)", self::d("tbl")->orWhereNotIn("foo", $subSelect)->query());
    }

    public function testOrderingMapping(): void
    {
        $ds = "DELETE FROM `table` ORDER BY";

        $this->assertEquals("$ds `name`", self::d("table")->order("name")->query());
        $this->assertEquals("$ds `age` DESC", self::d("table")->orderDesc("age")->query());
        $this->assertEquals("$ds LENGTH(name)", self::d("table")->orderExpr("LENGTH(name)")->query());
        $this->assertEquals("$ds LENGTH(name) DESC", self::d("table")->orderExprDesc("LENGTH(name)")->query());
    }

    public function testFullDeleteQuery(): void
    {
        $subSelect = new Select(new ConnectDebug(), "users", ["id"], ["status" => "inactive"]);

        $expectedQuery = "DELETE FROM `orders` AS `o` PARTITION (p1, p2) " .
            "WHERE `status`='pending' AND `o`.`amount`>100 " .
            "AND EXISTS (" . $subSelect->query() . ") " .
            "ORDER BY `o`.`created_at` DESC,LENGTH(`o`.`reference`) " .
            "LIMIT 50";

        $query = self::d("orders")
            ->setTableAlias("o")
            ->setPartitions("p1, p2")
            ->whereEq("status", "pending")
            ->whereGt("o`.`amount", 100) // Because column names are wrapped in ``, to get `o`.`amount` you need to write it as o`.`amount
            ->whereExists($subSelect)
            ->orderDesc("o`.`created_at") // Because column names are wrapped in ``, to get `o`.`created_at` you need to write it as o`.`created_at
            ->orderExpr("LENGTH(`o`.`reference`)")
            ->setLimit(50)
            ->query();

        $this->assertEquals($expectedQuery, $query);
    }
}
