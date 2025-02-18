<?php declare(strict_types=1);

namespace AP\Mysql\Tests\Statement;

use AP\Mysql\Statement\OrderBy;
use PHPUnit\Framework\TestCase;

class OrderByTest extends TestCase
{
    static public function o(): OrderBy
    {
        return new OrderBy();
    }

    public function testMain(): void
    {
        $this->assertEquals("", self::o()->query());
        $this->assertEquals("`name`", self::o()->asc("name")->query());
        $this->assertEquals("`name` DESC", self::o()->desc("name")->query());
        $this->assertEquals("2", self::o()->asc(2)->query());
        $this->assertEquals("2 DESC", self::o()->desc(2)->query());
        $this->assertEquals("SOME_EXPR()", self::o()->exprAsc("SOME_EXPR()")->query());

        $this->assertEquals(
            "DATE(`created_at`) DESC",
            self::o()->exprDesc("DATE(`created_at`)")->query()
        );

        $this->assertEquals(
            "DATE(`created_at`) DESC,`order`",
            self::o()->exprDesc("DATE(`created_at`)")->asc("order")->query()
        );
    }
}
