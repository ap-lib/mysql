<?php declare(strict_types=1);

namespace AP\Mysql\Connect;

use AP\Logger\Log;
use AP\Mysql\Raw;
use JsonException;
use mysqli;
use mysqli_result;
use mysqli_sql_exception;
use UnexpectedValueException;

class Connect implements ConnectInterface
{
    use ConnectStatements;

    readonly private mysqli $mysqli;
    private bool            $connected = false;

    public function __construct(
        readonly private string $hostname,
        readonly private string $username,
        readonly private string $password,
        readonly private string $scheme,
        readonly private int    $port = 3306,
        readonly private int    $connection_attempts = 2,
        readonly private float  $connection_timeout = 1.0, // TODO: double check float timeout is available
        readonly private array  $init_commands = [],
    )
    {
    }

    protected function log(string $query, float $start, ?float $runtime = null): void
    {
        Log::debug(
            $query,
            [
                'host'    => $this->hostname,
                'port'    => $this->port,
                'base'    => $this->scheme,
                'start'   => $start,
                'runtime' => is_null($runtime)
                    ? microtime(true) - $start
                    : $runtime,
            ],
            'ap:db'
        );
    }

    public function mockFakeConnection(string $charset)
    {
        $this->connected = true;
        $this->mysqli    = new mysqli();
        $this->mysqli->set_charset($charset);
    }

    /**
     * + Late connection
     * recommended no use driver directly
     *
     * @return mysqli
     * @throws mysqli_sql_exception
     */
    public function driver(): mysqli
    {
        if (!$this->connected) {
            if (empty($this->mysqli)) {
                $this->mysqli = new mysqli();
                $this->mysqli->set_opt(
                    MYSQLI_OPT_CONNECT_TIMEOUT,
                    $this->connection_timeout
                );
            }

            $iter  = 0;
            $start = microtime(true);
            while (true) {
                try {
                    $this->mysqli->real_connect(
                        $this->hostname,
                        $this->username,
                        $this->password,
                        $this->scheme,
                        $this->port
                    );
                    $this->connected = true;
                    $this->log('connect', $start);
                    break;
                } catch (mysqli_sql_exception $e) {
                    $iter++;
                    if ($iter >= $this->connection_attempts) {
                        throw $e;
                    }
                }
            }

            // init after connecting
            foreach ($this->init_commands as $command) {
                $this->exec($command);
            }
        }
        return $this->mysqli;
    }


    /**
     * Performs a query on the database
     *
     * @param string $query The query string. The data must be properly formatted, and all strings must be
     *                              escaped using the **mysqli_real_escape_string** function.
     *
     * @return mysqli_result|true   For successful queries which produce a result set, such as SELECT, SHOW, DESCRIBE
     *                              or EXPLAIN **mysqli_query** will return a **mysqli_result** object.
     *                              For other successful queries **mysqli_query** will return true.
     *
     * @throws mysqli_sql_exception For errors
     */
    public function exec(string $query): mysqli_result|true
    {
        $driver = $this->driver(); // because a late loading driver using important to get a driver before start to calculate time
        $start  = microtime(true);
        $res    = $driver->query($query);
        $this->log($query, $start);
        return $res;
    }

    /**
     * @throws JsonException
     */
    public function escape(mixed $value): string
    {
        // TODO: maybe need to add Interface-like code to be able to do custom escapes

        if (is_string($value)) {
            return "'{$this->driver()->real_escape_string($value)}'";
        }
        if (is_bool($value)) {
            return $value ? '1' : '0';
        }
        if (is_null($value)) {
            return 'null';
        }
        if (is_int($value) || is_float($value)) {
            return (string)$value;
        }
        if ($value instanceof Raw) {
            return $value->escape($this);
        }
        if (is_array($value)) {
            return json_encode($value, JSON_THROW_ON_ERROR);
        }

        throw new UnexpectedValueException('this value can\'t be escaped');
    }

    public function lastInsertId(): int
    {
        return (int)$this->driver()->insert_id;
    }

    public function lastAffectedRows(): int
    {
        return (int)$this->driver()->affected_rows;
    }
}