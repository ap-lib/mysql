# AP\Mysql

AP\Mysql is a ***performance-oriented*** query builder built exclusively for MySQL. It leverages MySQL-specific features
like INSERT IGNORE, REPLACE, ON DUPLICATE KEY UPDATE, partitions, and bulk operations for maximum efficiency

Unlike generic query builders, **AP\Mysql** does **not** attempt to be cross-database compatible. Instead, it is finely
tuned to leverage **MySQL's native syntax and optimizations** for maximum efficiency.

[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## Installation

```bash
composer require ap-lib/mysql
```

## Features

- Performance-oriented query builder for MySQL
- Supports INSERT, UPDATE, DELETE, SELECT, and REPLACE
- Bulk INSERT and REPLACE with query generators
- Secure escaping for values while keeping column names flexible
- Supports ON DUPLICATE KEY UPDATE
- Partitioning support

## Requirements

- PHP 8.3 or higher
- php-mysqli driver
- MySQL 8.4

## Getting started

### Connecting to the Database
```php
use AP\Mysql\Connect\Connect;

$connect = new Connect(
    'localhost', 
    'user', 
    'password', 
    'scheme'
);
```

### ⚠️ Important Security Notice

For **performance reasons**, **all names (table, column, partition, etc.) are not automatically escaped**.  
If you use **dynamic input**, manually sanitize names using:

```php
use AP\Mysql\Helper;

$safeName = Helper::escapeName($userInput);
```

#### ✅ Values (data) are always properly escaped and safe

### INSERT
```php
$insert = $connect->insert(
    'users',
    [
        'name' => "John Doe's market",
        'email' => 'john@example.com'
    ]
);

// execute 
$insert->exec();

// or get query: INSERT `users`(`name`,`email`) VALUE ('John Doe\'s market','john@example.com')
echo $query->query();


```

### INSERT BULK
```php
$bulkInsert = $connect->insertBulk('users', [
    ['name' => 'John Doe', 'email' => 'john@example.com'],
    ['name' => 'Jane Doe', 'email' => 'jane@example.com']
]);

// execute
$bulkInsert->exec();

// or get queries 
foreach ($bulkInsert->queries() as $query) {
    echo $query . PHP_EOL;
}
// INSERT `users`(`id`,`name`,`email`) VALUE (1,'John Doe','john@example.com'),(2,'Jane Doe','jane@example.com')
// by 1000 lines on one query by default
```

### INSERT ... SELECT
```php
$insert = $connect->insertSelect(
    "users",
    $connect->select('old_users', ['id', 'name', 'email']),
    ['id', 'username', 'email']
);

// execute
$bulkInsert->exec();

// or get query: INSERT `users`(`id`,`username`,`email`) SELECT `id`,`name`,`email` FROM `old_users`
echo $query;

```

### REPLACE
```php
$replace = $connect->replace(
    'users',
    [
        'id' => 1,
        'name' => 'John Doe',
        'email' => 'john@example.com'
    ]
);

// execute  
$replace->exec();

// or get query: REPLACE `users`(`id`,`name`,`email`) VALUE (1,'John Doe','john@example.com')
echo $replace->query();
```

### REPLACE BULK
```php
$bulkReplace = $connect->replaceBulk('users', [
    ['id' => 1, 'name' => 'John Doe', 'email' => 'john@example.com'],
    ['id' => 2, 'name' => 'Jane Doe', 'email' => 'jane@example.com']
]);

// execute  
$bulkReplace->exec();

// or get queries  
foreach ($bulkReplace->queries() as $query) {
    echo $query . PHP_EOL;
}
// REPLACE `users`(`id`,`name`,`email`) VALUE (1,'John Doe','john@example.com'),(2,'Jane Doe','jane@example.com')
// by 1000 lines on one query by default
```

### REPLACE ... SELECT
```php
$replace = $connect->replaceSelect(
    "users", 
    $connect->select('old_users', ['id', 'name', 'email']),
    ['id', 'username', 'email']
);

// execute  
$replace->exec();

// or get query: REPLACE `users`(`id`,`username`,`email`) SELECT `id`,`name`,`email` FROM `old_users`
echo $replace->query();
```

### DELETE
```php
$delete = $connect->delete("users", ['id' => 1]);

// execute  
$delete->exec();

// or get query: DELETE FROM `users` WHERE `id`=1
echo $delete->query();
```

### SELECT
```php
$select = $connect->select('users', ['id', 'name', 'email'])
    ->whereEq('status', 'active')
    ->order('name')
    ->setLimit(10);

// execute and get results  
$results = $select->exec();

// or get query: SELECT `id`,`name`,`email` FROM `users` WHERE `status`='active' ORDER BY `name` LIMIT 10
echo $select->query();
```

### UPDATE
```php
$update = $connect->update('users')
    ->assignment('status', 'paused')
    ->assignment('paused_at', new Raw('NOW()'))
    ->whereEq('id', 7);

// execute
$results = $update->exec();

// or get query: UPDATE `users` SET `status`='paused',`paused_at`=NOW() WHERE `id`=7
echo $update->query();
```

----

### WHERE Conditions
It allows chaining multiple conditions while ensuring values are properly escaped.  
It can be used as a **sub-condition** inside **SELECT, DELETE, and UPDATE** queries.

```php
$where = $connect->where()
    ->subWhere(
        $connect->where()
            ->whereEq('role', 'admin')
            ->whereGt('last_login', '2024-01-01')
    )
    ->orSubWhere(
        $connect->where()
            ->whereEq('role', 'moderator')
            ->whereLt('last_login', '2023-01-01')
    );
    


echo $where->query();
// OUTPUT: WHERE (`role`='admin' AND `last_login`>'2024-01-01') 
//         OR (`role`='moderator' AND `last_login`<'2023-01-01')
```

## WHERE, ORDER, and GROUP Usage

AP\Mysql is designed for **maximum performance**, so **WHERE, ORDER, and GROUP are not built into queries as objects**.  
Instead, **you pass conditions directly into methods**, ensuring **lightweight execution**.

| Feature   | Supported In  |
|-----------|--------------|
| **WHERE** | `SELECT`, `UPDATE`, `DELETE` |
| **HAVING** | `SELECT` (Same methods as WHERE, prefixed with `having`) |
| **ORDER BY** | `SELECT`, `UPDATE`, `DELETE` |
| **GROUP BY** | `SELECT` |

---

### WHERE Conditions
WHERE conditions are passed **directly** when building queries:

```php
$select = $connect->select('users', ['id', 'name'])
    ->whereEq('status', 'active')
    ->whereGt('created_at', '2024-01-01');

// OUTPUT: SELECT `id`, `name` FROM `users` WHERE `status`='active' AND `created_at`>'2024-01-01'
echo $select->query();
```

#### Supported WHERE Operators
All **WHERE methods** have equivalent **HAVING methods** (e.g., `whereEq()` → `havingEq()`).

| Method | SQL Equivalent | Example |
|--------|---------------|---------|
| whereEq($col, $val) | column = value | whereEq('id', 1) → WHERE \`id\`=1 |
| whereNotEq($col, $val) | column <> value | whereNotEq('id', 1) → WHERE \`id\`<>1 |
| whereGt($col, $val) | column > value | whereGt('score', 50) → WHERE \`score\`>50 |
| whereGte($col, $val) | column >= value | whereGte('score', 50) → WHERE \`score\`>=50 |
| whereLt($col, $val) | column < value | whereLt('score', 50) → WHERE \`score\`<50 |
| whereLte($col, $val) | column <= value | whereLte('score', 50) → WHERE \`score\`<=50 |
| whereLike($col, $val) | column LIKE value | whereLike('name', '%John%') → WHERE \`name\` LIKE '%John%' |
| whereNotLike($col, $val) | column NOT LIKE value | whereNotLike('name', 'John%') → WHERE \`name\` NOT LIKE 'John%' |
| whereIsNull($col) | column IS NULL | whereIsNull('deleted_at') → WHERE \`deleted_at\` IS NULL |
| whereIsNotNull($col) | column IS NOT NULL | whereIsNotNull('deleted_at') → WHERE \`deleted_at\` IS NOT NULL |
| whereBetween($col, $start, $end) | column BETWEEN start AND end | whereBetween('age', 18, 25) → WHERE \`age\` BETWEEN 18 AND 25 |
| whereIn($col, $array) | column IN (values) | whereIn('id', [1, 2, 3]) → WHERE \`id\` IN (1,2,3) |
| whereNotIn($col, $array) | column NOT IN (values) | whereNotIn('id', [1, 2, 3]) → WHERE \`id\` NOT IN (1,2,3) |
| whereExists($subquery) | EXISTS (subquery) | whereExists($select) → WHERE EXISTS (SELECT id FROM users WHERE active=1) |
| whereNotExists($subquery) | NOT EXISTS (subquery) | whereNotExists($select) → WHERE NOT EXISTS (SELECT id FROM users WHERE active=1) |
| whereCond($rawCondition, ...$values) | Custom condition | whereCond("age > %s AND score < %s", 18, 50) → WHERE age > 18 AND score < 50 |

### OR Conditions
For OR conditions, use the orWhere...() variants:

```php
$select = $connect->select('users', ['id', 'name'])
    ->whereEq('status', 'active')
    ->orWhereEq('role', 'moderator');

// OUTPUT: SELECT `id`,`name` FROM `users` WHERE `status`='active' OR `role`='moderator'
echo $select->query();
```


### ORDER BY
Sorting records in **SELECT, UPDATE, and DELETE** queries:

```php
$select = $connect->select('users', ['id', 'name'])
    ->whereEq('status', 'active')
    ->order('name')
    ->orderDesc('created_at');

// OUTPUT: SELECT `id`, `name` FROM `users` WHERE `status`='active' ORDER BY `name`, `created_at` DESC
echo $select->query();
```

#### Supported ORDER Methods
| Method | SQL Equivalent | Example                                            |
|--------|---------------|----------------------------------------------------|
| order($col) | ORDER BY column ASC | order('name') → ORDER BY \`name\`                  |
| orderDesc($col) | ORDER BY column DESC | orderDesc('created_at') → ORDER BY \`created_at\` DESC |
| orderExpr($expr) | ORDER BY expression | orderExpr('LENGTH(name)') → ORDER BY LENGTH(name)  |
| orderExprDesc($expr) | ORDER BY expression DESC | orderExprDesc('RAND()') → ORDER BY RAND() DESC     |

### GROUP BY
Used for aggregating records in **SELECT queries**:

```php
$select = $connect->select(
    'users', 
    [
        'role', 
        'user_count' => new Raw('COUNT(*)')
    ]
)
    ->group('role');

// OUTPUT: SELECT `role`,COUNT(*) AS `user_count` FROM `users` GROUP BY `role`
echo $select->query();
```

#### Supported GROUP Methods
| Method | SQL Equivalent | Example                                                   |
|--------|---------------|-----------------------------------------------------------|
| group($col) | GROUP BY column | group('category') → GROUP BY \`category\`                 |
| groupExpr($expr) | GROUP BY expression | groupExpr('YEAR(created_at)') → GROUP BY YEAR(created_at) |
