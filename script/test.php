<?php declare(strict_types=1);


use AP\Mysql\Connect\Connect;
use AP\Mysql\Executable\InsertBulk;

include __DIR__ . "/../vendor/autoload.php";;
microtime(true);

$connect = new Connect(
    "127.0.10.1",
    "app",
    "password",
    "auth",
);
$connect->driver();

//$iter = 10000;
//
//
//
//
//$start = microtime(true);
//for ($i = 0; $i < $iter; $i++) {
//    $connect->insert("test", ["id" => $i, "label" => "hello world"])->query();
//}
//echo sprintf("connect->insert()->query (late): %f\n", microtime(true) - $start);
//
//$start = microtime(true);
//for ($i = 0; $i < $iter; $i++) {
//    $connect->upsert->insert("test", ["id" => $i, "label" => "hello world"]);
//}
//echo sprintf("connect->upsert->indert: %f\n", microtime(true) - $start);
//
//die;

// connect->upsert->insertBulk: 0.035291     2097152.000000     2097152.000000

$rows = [];

for ($i = 3000; $i < 3050; $i++) {
    $rows[] = [
        "label"  => md5(microtime()),
        "label2" => sha1(microtime()),
        "label3" => microtime(),
        "label4" => (int)microtime(true),
    ];
}


$insertBulk = new InsertBulk($connect, "test", $rows);
$insertBulk->setBatch(48);

foreach ($insertBulk->queries() as $query){
    print_r( "$query\n\n");
}

//$start = microtime(true);
//for ($i = 0; $i < 10; $i++) {
//    $connect->upsert->insertBulk("test", $rows);
//}
//echo sprintf("connect->upsert->insertBulk: %f\n", microtime(true) - $start);

//$start = microtime(true);
//for ($i = 0; $i < 10; $i++) {
//    $connect->upsert->insertBulk2("test", $rows);
//}
//echo sprintf("connect->upsert->insertBulk2: %f\n", microtime(true) - $start);
//

die;
$label = md5(microtime());
$connect->insert("test", ["id" => 1, "label" => $label], true, ["label" => $label]);
var_export($connect->lastAffectedRows());
echo "\n";
var_export($connect->lastInsertId());
echo "\n";

die;

$iter = 10000;
$id   = 123;

class LTLike
{
    public function __construct(
        protected Connect $connect,
    )
    {
    }

    function delete($table, $where = null, $whereParams = []): string
    {
        $query = "DELETE" . " FROM `$table` ";

        if (is_string($where)) {
            if (func_num_args() > 3) {
                $whereParams = func_get_args();
                unset($whereParams[0], $whereParams[1]);
            }
            if (!is_array($whereParams)) {
                $whereParams = [$whereParams];
            }

            $query .= " WHERE " . $this->bind($where, $whereParams);
        } elseif (is_array($where) && !empty($where)) {
            $query .= " WHERE ";
            $add   = [];
            foreach ($where as $k => $v) {
                $add[] = "`$k`=" . $this->connect->escape($v);
            }
            $query .= implode(" and ", $add);
        }

        return $query;
    }

    public function bind($string, $params)
    {
        $string = (string)$string;
        if (!is_array($params)) {
            $params = [$params];
        }

        if (strlen($string)) {
            /***************
             * TYPES:
             * 1 - search
             * 2 - in ""
             * 3 - in ''
             */

            $positions = [];
            $type      = 1;

            for ($i = 0; $i < strlen($string); $i++) {
                $s = substr($string, $i, 1);

                if ($type == 1) {
                    if ($s == "?") {
                        $positions[] = $i;
                    } elseif ($s == '"') {
                        $type = 2;
                    } elseif ($s == "'") {
                        $type = 3;
                    } elseif ($s == "\\") {
                        $i++;
                    }
                } elseif ($type == 2) {
                    if ($s == '"') {
                        $type = 1;
                    } elseif ($s == "\\") {
                        $i++;
                    }
                } elseif ($type == 3) {
                    if ($s == "'") {
                        $type = 1;
                    } elseif ($s == "\\") {
                        $i++;
                    }
                }
            }

            if (count($positions) != count($params)) {
                throw new Exception(
                    'Error when binding parameters with the query: ' . $string .
                    ' positions: ' . count($positions) .
                    ' params count: ' . count($params)
                );
            }

            if (!empty($params)) {
                $add = 0;
                $i   = 0;
                foreach ($params as $el) {
                    $value  = $this->connect->escape($el);
                    $string =
                        substr($string, 0, $positions[$i] + $add) .
                        $value .
                        substr($string, $positions[$i] + 1 + $add);

                    $add += strlen($value) - 1;
                    $i++;
                }
            }
        }
        return $string;
    }
}

for ($i = 0; $i < 1; $i++) {
    $connect->delete("user", ["id" => 1])->query();
}

$start = microtime(true);
for ($i = 0; $i < $iter; $i++) {
    $connect->delete("user", ["id" => 1])->query();
}
echo sprintf("connect->delete: %f\n", microtime(true) - $start);

$start = microtime(true);
for ($i = 0; $i < $iter; $i++) {
    $delete = $connect->delete("user");
    $delete->getWhere()->eq("id", $id);
    $delete->query();
}
echo sprintf("where->eq: %f\n", microtime(true) - $start);

$start = microtime(true);
for ($i = 0; $i < $iter; $i++) {
    $delete = $connect->delete("user");
    $delete->getWhere()->cond("id=%s", $id);
    $delete->query();
}
echo sprintf("where->where: %f\n", microtime(true) - $start);

$obj = new LTLike($connect);

$start = microtime(true);
for ($i = 0; $i < $iter; $i++) {
    $obj->delete("user", "id=?", $id);
}
echo sprintf("LTLike str: %f\n", microtime(true) - $start);


$start = microtime(true);
for ($i = 0; $i < $iter; $i++) {
    $obj->delete("user", ["id" => $id]);
}
echo sprintf("LTLike arr: %f\n", microtime(true) - $start);

//
//
$webmaster = 123;
$campaign  = 321;
//
//echo (new Where($connect, true))
//    ->sub(function (Where $where) use ($webmaster) {
//        return $where
//            ->eq("webmaster", 0)
//            ->eq("webmaster", $webmaster);
//    })
//    ->sub(fn(Where $sub) => $sub
//        ->eq("campaign", 0)
//        ->eq("campaign", $campaign)
//    )
//    ->query();
//
//echo (new \AP\Mysql\Where($connect, true))
//    ->in("webmaster", [0, $webmaster])
//    ->in("campaign", [0, $campaign])
//    ->query();

//echo (new \AP\Mysql\Where($connect, true))
//    ->sub(fn(\AP\Mysql\Where $sub) => $sub
//        ->eq("webmaster", 0)
//        ->eq("webmaster", $webmaster)
//    )
//    ->sub(fn(\AP\Mysql\Where $sub) => $sub
//        ->eq("campaign", 0)
//        ->eq("campaign", $campaign)
//    )
//    ->query();

//echo (new \AP\Mysql\Where($connect, true))
//    ->between("date", "2024-01-01", "2024-10-01")
//    ->eq("status", 1)
//    ->eq("status", 1) // status = 1
//    ->notEq("status", 0) // status <> 0
//    ->gt("age", 18) // age > 18
//    ->gte("salary", 50000) // salary >= 50000
//    ->lt("age", 60) // age < 60
//    ->lte("discount", 20) // discount <= 20
//    ->between("date", "2024-01-01", "2024-10-01") // date BETWEEN '2024-01-01' AND '2024-10-01'
//    ->in("category", ["tech", "health", "finance"]) // category IN ('tech', 'health', 'finance')
//    ->notIn("status", [0, 2, 3]) // status NOT IN (0, 2, 3)
//    ->like("username", "admin%") // username LIKE 'admin%'
//    ->notLike("email", "%@spam.com") // email NOT LIKE '%@spam.com'
//    ->isNull("deleted_at") // deleted_at IS NULL
//    ->isNotNull("updated_at")
//    ->query();

//$where->add("`date` between ? and ?", "2024-01-01", "2025-01-01");
//
//$db = new Connect(
//    "127.0.10.1",
//    "app",
//    "password",
//    "auth",
//);
//
//print_r($db->query("select id, guid, type, label, created_at, created_by from users_base")->fetch_all(MYSQLI_ASSOC));
//
//
//$query = (string)(new \AP\Mysql\Delete($db, "users_base"));
//
//echo "\n\n$query;\n\n";


//
//$delete = new \AP\Mysql\Delete($db, "tbl");
//$delete->where->add()
//
//
//echo $delete->query();