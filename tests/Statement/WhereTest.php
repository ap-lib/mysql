<?php declare(strict_types=1);

namespace AP\Mysql\Tests\Statement;

use AP\Mysql\Connect\ConnectDebug;
use AP\Mysql\Executable\Select;
use AP\Mysql\Statement\Where;
use PHPUnit\Framework\TestCase;

class WhereTest extends TestCase
{
    static public function w(): Where
    {
        return new Where(new ConnectDebug());
    }

    public function testMain(): void
    {
        // blank
        $this->assertEquals("", self::w()->query());

        // first element no matter and/or
        $this->assertEquals("`name`='hello'", self::w()->eq("name", "hello")->query());
        $this->assertEquals("`name`='hello'", self::w()->orEq("name", "hello")->query());

        // for second and all next it affects to sepeartor
        $this->assertEquals(
            "`f`=1 AND `s`=2",
            self::w()
                ->eq("f", 1)
                ->eq("s", 2)
                ->query()
        );
        $this->assertEquals(
            "`f`=1 OR  `s`=2",
            self::w()
                ->eq("f", 1)
                ->orEq("s", 2)
                ->query()
        );
        $this->assertEquals(
            "`f`=1 AND `s`=2 OR  `t`=3",
            self::w()
                ->eq("f", 1)
                ->eq("s", 2)
                ->orEq("t", 3)
                ->query()
        );
    }

    public function testMethods(): void
    {
        $subSelect = new Select(new ConnectDebug());

        $this->assertEquals("(foo='boo' or foo in null)", self::w()->cond("foo=%s or foo in null", "boo")->query());
        $this->assertEquals("(foo=1 or foo in null)", self::w()->cond("foo=%s or foo in null", 1)->query());
        $this->assertEquals("`foo`='boo'", self::w()->eq("foo", "boo")->query());
        $this->assertEquals("`foo`<>'boo'", self::w()->notEq("foo", "boo")->query());
        $this->assertEquals("`foo`>1", self::w()->gt("foo", 1)->query());
        $this->assertEquals("`foo`<1", self::w()->lt("foo", 1)->query());
        $this->assertEquals("`foo`>=1", self::w()->gte("foo", 1)->query());
        $this->assertEquals("`foo`<=1", self::w()->lte("foo", 1)->query());
        $this->assertEquals("`foo` LIKE '%boo'", self::w()->like("foo", "%boo")->query());
        $this->assertEquals("`foo` NOT LIKE 'boo%'", self::w()->notLike("foo", "boo%")->query());
        $this->assertEquals("`foo` IS NULL", self::w()->isNull("foo")->query());
        $this->assertEquals("`foo` IS NOT NULL", self::w()->isNotNull("foo")->query());
        $this->assertEquals("`foo` BETWEEN 1 AND 2", self::w()->between("foo", 1, 2)->query());
        $this->assertEquals("`foo` IN (1,2)", self::w()->in("foo", [1, 2])->query());
        $this->assertEquals("`foo` NOT IN (1,2)", self::w()->notIn("foo", [1, 2])->query());
        $this->assertEquals("(`foo`=1)", self::w()->subWhere(self::w()->eq("foo", 1))->query());
        $this->assertEquals("(`foo`=1)", self::w()->subFn(fn(Where $sub) => $sub->eq("foo", 1))->query());
        // TODO: change to "assertEquals" after select will be done
        $this->assertStringStartsWith("EXISTS (", self::w()->exists($subSelect)->query());
        $this->assertStringStartsWith("NOT EXISTS (", self::w()->notExists($subSelect)->query());
        $this->assertStringStartsWith("`foo` IN (", self::w()->in("foo", $subSelect)->query());
        $this->assertStringStartsWith("`foo` NOT IN (", self::w()->notIn("foo", $subSelect)->query());


        $this->assertEquals("(foo='boo' or foo in null)", self::w()->orCond("foo=%s or foo in null", "boo")->query());
        $this->assertEquals("(foo=1 or foo in null)", self::w()->orCond("foo=%s or foo in null", 1)->query());
        $this->assertEquals("`foo`='boo'", self::w()->orEq("foo", "boo")->query());
        $this->assertEquals("`foo`<>'boo'", self::w()->orNotEq("foo", "boo")->query());
        $this->assertEquals("`foo`>1", self::w()->orGt("foo", 1)->query());
        $this->assertEquals("`foo`<1", self::w()->orLt("foo", 1)->query());
        $this->assertEquals("`foo`>=1", self::w()->orGte("foo", 1)->query());
        $this->assertEquals("`foo`<=1", self::w()->orLte("foo", 1)->query());
        $this->assertEquals("`foo` LIKE '%boo'", self::w()->orLike("foo", "%boo")->query());
        $this->assertEquals("`foo` NOT LIKE 'boo%'", self::w()->orNotLike("foo", "boo%")->query());
        $this->assertEquals("`foo` IS NULL", self::w()->orIsNull("foo")->query());
        $this->assertEquals("`foo` IS NOT NULL", self::w()->orIsNotNull("foo")->query());
        $this->assertEquals("`foo` BETWEEN 1 AND 2", self::w()->orBetween("foo", 1, 2)->query());
        $this->assertEquals("`foo` IN (1,2)", self::w()->orIn("foo", [1, 2])->query());
        $this->assertEquals("`foo` NOT IN (1,2)", self::w()->orNotIn("foo", [1, 2])->query());
        $this->assertEquals("(`foo`=1)", self::w()->orSubWhere(self::w()->eq("foo", 1))->query());
        $this->assertEquals("(`foo`=1)", self::w()->orSubFn(fn(Where $sub) => $sub->eq("foo", 1))->query());
        // TODO: change to "assertEquals" after select will be done
        $this->assertStringStartsWith("EXISTS (", self::w()->orExists($subSelect)->query());
        $this->assertStringStartsWith("NOT EXISTS (", self::w()->orNotExists($subSelect)->query());
        $this->assertStringStartsWith("`foo` IN (", self::w()->orIn("foo", $subSelect)->query());
        $this->assertStringStartsWith("`foo` NOT IN (", self::w()->orNotIn("foo", $subSelect)->query());
    }
}
