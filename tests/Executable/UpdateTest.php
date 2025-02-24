<?php declare(strict_types=1);

namespace AP\Mysql\Tests\Executable;

use AP\Mysql\Connect\ConnectDebug;
use AP\Mysql\Executable\Select;
use AP\Mysql\Executable\Update;
use AP\Mysql\Raw;
use AP\Mysql\Statement\Where;
use PHPUnit\Framework\TestCase;

class UpdateTest extends TestCase
{
    static public function u(
        string $table,
    ): Update
    {
        return new Update(
            new ConnectDebug(),
            $table,
        );
    }

    static public function w(): Where
    {
        return new Where(new ConnectDebug());
    }

    public function testBasicSelect(): void
    {
        $ds = /** @lang text */
            "UPDATE `table` SET `a`='hello'";

        $this->assertEquals("$ds", self::u("table")->assignment("a", "hello")->query());
        $this->assertEquals("$ds LIMIT 10", self::u("table")->assignment("a", "hello")->setLimit(10)->query());
        $this->assertEquals("$ds LIMIT 0", self::u("table")->assignment("a", "hello")->setLimit(0)->query());

        // option with error, be performance reason it works with no validation
        $this->assertEquals("$ds LIMIT -10", self::u("table")->assignment("a", "hello")->setLimit(-10)->query());

        $this->assertEquals(
        /** @lang text */ "UPDATE `table` PARTITION (p1, p2) SET `a`='hello'",
            self::u("table")->assignment("a", "hello")->setPartitions("p1, p2")->query()
        );
        $this->assertEquals(
        /** @lang text */ "UPDATE `table` AS `t` SET `a`='hello'",
            self::u("table")->assignment("a", "hello")->setTableAlias("t")->query()
        );
        $this->assertEquals(
        /** @lang text */ "UPDATE `table` AS `t` PARTITION (p1, p2) SET `a`='hello'",
            self::u("table")->assignment("a", "hello")->setTableAlias("t")->setPartitions("p1, p2")->query()
        );
    }

    public function testWhere(): void
    {
        $subSelect     = (new Select(new ConnectDebug(), "subtable", ["id"]))
            ->whereEq("name", "Yuri");
        $subSelectText = $subSelect->query();

        $base = self::u("table")->assignment("a", "hello");

        $ds = /** @lang text */
            "UPDATE `table` SET `a`='hello' WHERE";

        $this->assertEquals("$ds (foo='boo' or foo in null)", (clone $base)->whereCond("foo=%s or foo in null", "boo")->query());
        $this->assertEquals("$ds (foo=1 or foo in null)", (clone $base)->whereCond("foo=%s or foo in null", 1)->query());
        $this->assertEquals("$ds `foo`='boo'", (clone $base)->whereEq("foo", "boo")->query());
        $this->assertEquals("$ds `foo`<>'boo'", (clone $base)->whereNotEq("foo", "boo")->query());
        $this->assertEquals("$ds `foo`>1", (clone $base)->whereGt("foo", 1)->query());
        $this->assertEquals("$ds `foo`<1", (clone $base)->whereLt("foo", 1)->query());
        $this->assertEquals("$ds `foo`>=1", (clone $base)->whereGte("foo", 1)->query());
        $this->assertEquals("$ds `foo`<=1", (clone $base)->whereLte("foo", 1)->query());
        $this->assertEquals("$ds `foo` LIKE '%boo'", (clone $base)->whereLike("foo", "%boo")->query());
        $this->assertEquals("$ds `foo` NOT LIKE 'boo%'", (clone $base)->whereNotLike("foo", "boo%")->query());
        $this->assertEquals("$ds `foo` IS NULL", (clone $base)->whereIsNull("foo")->query());
        $this->assertEquals("$ds `foo` IS NOT NULL", (clone $base)->whereIsNotNull("foo")->query());
        $this->assertEquals("$ds `foo` BETWEEN 1 AND 2", (clone $base)->whereBetween("foo", 1, 2)->query());
        $this->assertEquals("$ds `foo` IN (1,2)", (clone $base)->whereIn("foo", [1, 2])->query());
        $this->assertEquals("$ds `foo` NOT IN (1,2)", (clone $base)->whereNotIn("foo", [1, 2])->query());
        $this->assertEquals("$ds (`foo`=1)", (clone $base)->whereSubWhere(self::w()->eq("foo", 1))->query());
        $this->assertEquals("$ds (`foo`=1)", (clone $base)->whereSubFn(fn(Where $sub) => $sub->eq("foo", 1))->query());
        $this->assertEquals("$ds EXISTS ($subSelectText)", (clone $base)->whereExists($subSelect)->query());
        $this->assertEquals("$ds NOT EXISTS ($subSelectText)", (clone $base)->whereNotExists($subSelect)->query());
        $this->assertEquals("$ds `foo` IN ($subSelectText)", (clone $base)->whereIn("foo", $subSelect)->query());
        $this->assertEquals("$ds `foo` NOT IN ($subSelectText)", (clone $base)->whereNotIn("foo", $subSelect)->query());


        $this->assertEquals("$ds (foo='boo' or foo in null)", (clone $base)->orWhereCond("foo=%s or foo in null", "boo")->query());
        $this->assertEquals("$ds (foo=1 or foo in null)", (clone $base)->orWhereCond("foo=%s or foo in null", 1)->query());
        $this->assertEquals("$ds `foo`='boo'", (clone $base)->orWhereEq("foo", "boo")->query());
        $this->assertEquals("$ds `foo`<>'boo'", (clone $base)->orWhereNotEq("foo", "boo")->query());
        $this->assertEquals("$ds `foo`>1", (clone $base)->orWhereGt("foo", 1)->query());
        $this->assertEquals("$ds `foo`<1", (clone $base)->orWhereLt("foo", 1)->query());
        $this->assertEquals("$ds `foo`>=1", (clone $base)->orWhereGte("foo", 1)->query());
        $this->assertEquals("$ds `foo`<=1", (clone $base)->orWhereLte("foo", 1)->query());
        $this->assertEquals("$ds `foo` LIKE '%boo'", (clone $base)->orWhereLike("foo", "%boo")->query());
        $this->assertEquals("$ds `foo` NOT LIKE 'boo%'", (clone $base)->orWhereNotLike("foo", "boo%")->query());
        $this->assertEquals("$ds `foo` IS NULL", (clone $base)->orWhereIsNull("foo")->query());
        $this->assertEquals("$ds `foo` IS NOT NULL", (clone $base)->orWhereIsNotNull("foo")->query());
        $this->assertEquals("$ds `foo` BETWEEN 1 AND 2", (clone $base)->orWhereBetween("foo", 1, 2)->query());
        $this->assertEquals("$ds `foo` IN (1,2)", (clone $base)->orWhereIn("foo", [1, 2])->query());
        $this->assertEquals("$ds `foo` NOT IN (1,2)", (clone $base)->orWhereNotIn("foo", [1, 2])->query());
        $this->assertEquals("$ds (`foo`=1)", (clone $base)->orWhereSubWhere(self::w()->eq("foo", 1))->query());
        $this->assertEquals("$ds (`foo`=1)", (clone $base)->orWhereSubFn(fn(Where $sub) => $sub->eq("foo", 1))->query());
        $this->assertEquals("$ds EXISTS ($subSelectText)", (clone $base)->orWhereExists($subSelect)->query());
        $this->assertEquals("$ds NOT EXISTS ($subSelectText)", (clone $base)->orWhereNotExists($subSelect)->query());
        $this->assertEquals("$ds `foo` IN ($subSelectText)", (clone $base)->orWhereIn("foo", $subSelect)->query());
        $this->assertEquals("$ds `foo` NOT IN ($subSelectText)", (clone $base)->orWhereNotIn("foo", $subSelect)->query());
    }

    public function testOrderingMapping(): void
    {
        $ds = /** @lang text */
            "UPDATE `table` SET `a`='hello' ORDER BY";

        $base = self::u("table")->assignment("a", "hello");

        $this->assertEquals("$ds `name`", (clone $base)->order("name")->query());
        $this->assertEquals("$ds `age` DESC", (clone $base)->orderDesc("age")->query());
        $this->assertEquals("$ds LENGTH(name)", (clone $base)->orderExpr("LENGTH(name)")->query());
        $this->assertEquals("$ds LENGTH(name) DESC", (clone $base)->orderExprDesc("LENGTH(name)")->query());
    }

    public function testFullUpdateQuery(): void
    {
        $subSelect = (new Select(new ConnectDebug(), "users", ["id"]))
            ->whereEq("status", "active");

        $expectedQuery = /** @lang text */
            "UPDATE IGNORE `orders_arch` AS `o` PARTITION (p1, p2) " .
            "SET `title`='Hans Christian Andersen\'s Complete Fairy Tales',`updates_count`=`updates_count`+1 " .
            "WHERE `status`='pending' AND `o`.`amount`>100 " .
            "AND EXISTS (" . $subSelect->query() . ") " .
            "ORDER BY `o`.`created_at` DESC,LENGTH(`o`.`reference`) " .
            "LIMIT 50";

        $query = self::u("orders")
            ->assignment("title", "Hans Christian Andersen's Complete Fairy Tales") // safe value, no save name
            ->assignment("updates_count", new Raw("`updates_count`+1")) // no save name & value
            ->setIgnore()
            ->setTableAlias("o")
            ->setPartitions("p1, p2")
            ->whereEq("status", "pending")
            ->whereGt("o`.`amount", 100) // Because column names are wrapped in ``, to get `o`.`amount` you need to write it as o`.`amount
            ->whereExists($subSelect)
            ->orderDesc("o`.`created_at") // Because column names are wrapped in ``, to get `o`.`created_at` you need to write it as o`.`created_at
            ->orderExpr("LENGTH(`o`.`reference`)")
            ->setLimit(50)
            ->setTable("orders_arch")
            ->query();

        $this->assertEquals($expectedQuery, $query);
    }
}
