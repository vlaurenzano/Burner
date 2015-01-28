<?php

namespace Burner;

/**
 * Description of Burner
 *
 * @author vlaurenzano
 */
class Burner {

  /**
   * Database instance
   * @var \PDO 
   */
  protected $pdo;

  /**
   * The table name of our record
   * @var string 
   */
  protected $table;

  /**
   * The original data from source, will be empty if new record 
   * @var type 
   */
  protected $orignalData;

  /**
   * Contains current data
   * @var array $data
   */
  protected $data;
  
  /**
   * Primary where array
   * @var type 
   */
  protected $uniqueIdentifier;

  /**
   * Static cache of table maps
   * @var array 
   */
  static $tableMapsCache = array();

  /**
   * Static cache of uniqueIndexes
   */
  static $primaryKeyCache = array();  
  
  /**
   * Takes in an instance of pdo, the table we are persisting, and or variable argument
   * @param \PDO $db instance of pdo
   * @param type $table the table we are using
   * @param type $mixed either a primary key value, an array, or null 
   */
  public function __construct(\PDO $pdo, $table, $mixed = null) {
    $this->pdo = $pdo;
    $this->table = $table;
    $this->fetchAndSetDbInfo();
    if ($mixed) {
      $this->handleMixedParams($mixed);
    }
  }

  /**
   * If array of same length, sets it as the data without querying
   * If different lenth, queries the db
   * If not array tries to use value as primary key 
   * @param mixed $mixed
   * @throws \RuntimeException
   */
  protected function handleMixedParams($mixed) {
    if (is_array($mixed)) {
      //checks if all the table columns are present in the input ignoring order. 
      //Alternative check would have been sorting both array's keys and comparing     
      if (!array_diff_key($mixed, self::$tableMapsCache[$this->table]) && count($mixed) === count(self::$tableMapsCache[$this->table])) {
        $this->orignalData = $mixed;
        $this->data = $mixed;
        return;
      }
      $this->queryOne($mixed);
    }
    if( !self::$primaryKeyCache[$this->table] ){
      throw new Exceptions\BurnerException('Cannot infer primary key for ' . $this->table .'.');
    }
    $this->fetchOne(array(self::$primaryKeyCache[$this->table] => $mixed));
  }

  /**
   * Checks cache to see if data already there, else fetches from db
   */
  protected function fetchAndSetDbInfo() {
    if (!isset(self::$tableMapsCache[$this->table])) {
      self::$tableMapsCache[$this->table] = $this->fetchTableMap();
    }
    if (!isset(self::$primaryKeyCache[$this->table])) {
      self::$primaryKeyCache[$this->table] = $this->fetchPrimary();
    }
  }

  /**
   * Returns the table map from the db
   * @return the table map
   * @throws \RuntimeException if the given table is not found
   */
  protected function fetchTableMap() {
    $fields = $this->pdo->query("SHOW FIELDS from $this->table")->fetchAll(\PDO::FETCH_ASSOC);
    if (!$fields) {
      throw new Exceptions\BurnerException('Table ' . $this->table . ' does not exist.');
    }
    //we only really care about the column names
    return array_reduce($fields, function( $carry, $column ) {
      $carry[$column['Field']] = $column;
      return $carry;
    }, array());
  }

  /**
   * Fetches the primary key from the db
   * @return boolean
   */
  protected function fetchPrimary() {
    $key = $this->pdo->query("SHOW KEYS FROM {$this->table} WHERE Key_name = 'PRIMARY'")->fetch(\PDO::FETCH_ASSOC);
    if ($key) {
      return $key['Column_name'];
    }
    throw new Exceptions\BurnerException();
  }
  
  /**
   * Get's one result from the database
   * @param array $where
   */
  protected function fetchOne(array $where) {
    $stmt = $this->buildFetchQueryStatement($where);      
    $data = $stmt->fetch(\PDO::FETCH_ASSOC);    
    if( $data ) {
      $this->orignalData = $data;
      $this->data = $data;
      $this->uniqueIdentifier = array(self::$primaryKeyCache[$this->table] => $data[self::$primaryKeyCache[$this->table]]);          
    }
  }
  
  
  /**
   * Builds the query statement to get record(s)
   * @param array $where
   * @param type $execute
   * @return type
   */
  protected function buildFetchQueryStatement( array $where, $execute = TRUE ){
    $sqlWhereArray = array_map( function($key){ return "$key = :$key";}, array_keys($where));    
    $stmt = $this->pdo->prepare( '
        SELECT *
        FROM ' . $this->table . '
        WHERE ' . implode( ' AND ', $sqlWhereArray ) );
    foreach($where as $k => $v){
      $stmt->bindParam(":$k",$v  );
    }
    if($execute){
      $stmt->execute();
    }
    return $stmt;
  }

}
