<?php declare(strict_types=1);

namespace AP\Mysql\Tests;

use AP\Mysql\Connect\ConnectDebug;
use AP\Mysql\Executable\Update;
use AP\Mysql\Raw;
use AP\Mysql\Statement\Where;
use PHPUnit\Framework\TestCase;

class ReadmeTest extends TestCase
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

    public function testInsert(): void
    {
        $connect = new ConnectDebug();

        $insert = $connect->insert(
            'users',
            [
                'name'  => "John Doe's market",
                'email' => 'john@example.com'
            ]
        );

        $this->assertEquals(
            "INSERT `users`(`name`,`email`) VALUE ('John Doe\'s market','john@example.com')",
            $insert->query()
        );
    }

    public function testInsertBulk(): void
    {
        $connect = new ConnectDebug();

        $replace = $connect->insertBulk('users', [
            ['id' => 1, 'name' => 'John Doe', 'email' => 'john@example.com'],
            ['id' => 2, 'name' => 'Jane Doe', 'email' => 'jane@example.com']
        ]);

        $this->assertEquals(
            "INSERT `users`(`id`,`name`,`email`) VALUE (1,'John Doe','john@example.com'),(2,'Jane Doe','jane@example.com')",
            implode(";", iterator_to_array($replace->queries()))
        );
    }

    public function testInsertSelect(): void
    {
        $connect = new ConnectDebug();

        $insert = $connect->insertSelect(
            "users",
            $connect->select('old_users', ['id', 'name', 'email']),
            ['id', 'username', 'email']
        );

        $this->assertEquals(
            "INSERT `users`(`id`,`username`,`email`) SELECT `id`,`name`,`email` FROM `old_users`",
            $insert->query()
        );
    }

    public function testReplace(): void
    {
        $connect = new ConnectDebug();

        $replace = $connect->replace(
            'users',
            [
                'id'    => 1,
                'name'  => 'John Doe',
                'email' => 'john@example.com'
            ]
        );

        $this->assertEquals(
            "REPLACE `users`(`id`,`name`,`email`) VALUE (1,'John Doe','john@example.com')",
            $replace->query()
        );
    }

    public function testReplaceBulk(): void
    {
        $connect = new ConnectDebug();

        $replace = $connect->replaceBulk('users', [
            ['id' => 1, 'name' => 'John Doe', 'email' => 'john@example.com'],
            ['id' => 2, 'name' => 'Jane Doe', 'email' => 'jane@example.com']
        ]);

        $this->assertEquals(
            "REPLACE `users`(`id`,`name`,`email`) VALUE (1,'John Doe','john@example.com'),(2,'Jane Doe','jane@example.com')",
            implode(";", iterator_to_array($replace->queries()))
        );
    }

    public function testReplaceSelect(): void
    {
        $connect = new ConnectDebug();

        $insert = $connect->replaceSelect(
            "users",
            $connect->select('old_users', ['id', 'name', 'email']),
            ['id', 'username', 'email']
        );

        $this->assertEquals(
            "REPLACE `users`(`id`,`username`,`email`) SELECT `id`,`name`,`email` FROM `old_users`",
            $insert->query()
        );
    }

    public function testDelete(): void
    {
        $connect = new ConnectDebug();

        $delete = $connect->delete("users")
            ->whereEq("id", 1);

        $this->assertEquals(
        /** @lang text */ "DELETE FROM `users` WHERE `id`=1",
            $delete->query()
        );
    }

    public function testSelect(): void
    {
        $connect = new ConnectDebug();

        $select = $connect->select('users', ['id', 'name', 'email'])
            ->whereEq('status', 'active')
            ->order('name')
            ->setLimit(10);

        $this->assertEquals(
        /** @lang text */ "SELECT `id`,`name`,`email` FROM `users` WHERE `status`='active' ORDER BY `name` LIMIT 10",
            $select->query()
        );
    }

    public function testUpdate(): void
    {
        $connect = new ConnectDebug();

        $update = $connect->update('users')
            ->assignment('status', 'paused')
            ->assignment('paused_at', new Raw('NOW()'))
            ->whereEq('id', 7);

        $this->assertEquals(
        /** @lang text */ "UPDATE `users` SET `status`='paused',`paused_at`=NOW() WHERE `id`=7",
            $update->query()
        );
    }

    public function testSelectGroupBy(): void
    {
        $connect = new ConnectDebug();

        $select = $connect->select('users', ['role', 'user_count' => new Raw('COUNT(*)')])
            ->group('role');

        $this->assertEquals(
        /** @lang text */ "SELECT `role`,COUNT(*) AS `user_count` FROM `users` GROUP BY `role`",
            $select->query()
        );
    }
}
