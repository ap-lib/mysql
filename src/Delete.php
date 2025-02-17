<?php declare(strict_types=1);

namespace AP\Mysql;

class Delete implements Statement, Executable
{

    private string $partitions  = "";
    private bool   $ignore      = false;
    private string $table_alias = '';

    /**
     * @param ConnectInterface $connect
     * @param string $table
     * @param Where|array<string|mixed>|null $where
     *       ***Recommended to use an array*** because it must performance-focused way,
     *       for example, if you want to delete, "where `id`=123", use ["id" => 123]
     *       If you will set Where object but no set it up, it will follow to error on SQL
     *       be performance reason this class no check it and expect to have:
     *          - null
     *          - non-empty array
     *          - non-empty Where object
     * @param OrderBy|null $order
     * @param int|null $limit // use limit only 0+, be performance reason this class no check what limit is valid
     */
    public function __construct(
        private readonly ConnectInterface $connect,
        private string                    $table,
        private Where|array|null          $where = null,
        private ?OrderBy                  $order = null,
        private ?int                      $limit = null,
    )
    {
    }

    public function setPartitions(string $partitions): static
    {
        $this->partitions = $partitions;
        return $this;
    }

    public function setTable(string $table): static
    {
        $this->table = $table;
        return $this;
    }

    public function setTableAlias(string $table_alias): static
    {
        $this->table_alias = $table_alias;
        return $this;
    }

    public function setIgnore(bool $ignore): static
    {
        $this->ignore = $ignore;
        return $this;
    }

    /**
     * @param int|null $limit use limit only 0+, be performance reason this class no check what limit is valid
     * @return $this
     */
    public function setLimit(?int $limit): static
    {
        $this->limit = $limit;
        return $this;
    }

    /**
     * important: it'll remove all previous sets where conditions
     *
     * @param Where|array|null $where
     * @return $this
     */
    public function setWhere(Where|array|null $where): static
    {
        $this->where = $where;
        return $this;
    }

    /**
     * use it only if you want to add changes to where, because it always will convert to Where object
     * it can reduce performance
     *
     * @return Where
     */
    public function getWhere(): Where
    {
        if (is_null($this->where)) {
            $this->where = new Where($this->connect);
        } elseif (is_array($this->where)) {
            $where = new Where($this->connect);
            foreach ($this->where as $k => $v) {
                $where->eq($k, $v);
            }
            $this->where = $where;
        }
        return $this->where;
    }

    /**
     * important: it'll remove all previous sets "order by" conditions
     *
     * @param OrderBy|null $order
     * @return $this
     */
    public function setOrder(?OrderBy $order): static
    {
        $this->order = $order;
        return $this;
    }

    /**
     * use it only if you want to add changes to "order by", because it always will convert to OrderBy object
     * it can reduce performance
     *
     * @return OrderBy
     */
    public function getOrder(): OrderBy
    {
        return is_null($this->order)
            ? $this->order = new OrderBy()
            : $this->order;
    }

    public function query(): string
    {
        $where_str = "";
        if (is_array($this->where)) {
            // A performance-focused, simplified version with an array-like structure for $where
            // expected to be the most frequently used option
            if (!empty($this->where)) {
                $where = [];
                foreach ($this->where as $k => $v) {
                    $where[] = "(`$k`={$this->connect->escape($v)})";
                }
                $where_str = ' WHERE ' . implode(" AND ", $where);
            }
        } elseif ($this->where instanceof Where) {
            $where_str = " WHERE {$this->where->query()}";
        }

        return 'DELETE' .
            ($this->ignore ? ' IGNORE' : '') .
            " FROM `$this->table`" .
            (empty($this->table_alias) ? '' : " AS `$this->table_alias`") .
            (empty($this->partitions) ? '' : " PARTITION $this->partitions") .
            $where_str .
            ($this->order instanceof OrderBy ? " ORDER BY {$this->order->query()}" : '') .
            (is_int($this->limit) ? " LIMIT $this->limit" : '');
    }

    /**
     * @return true
     */
    public function exec(): true
    {
        return $this->connect->exec($this->query());
    }
}