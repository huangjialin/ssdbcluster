<?php

require_once 'SsdbCluster.php';

$config = array(
    "master" => array(
                    array('host'=>'127.0.0.1', 'port'=>8888, "timeout"=>"10", "persistent_id"=>"", "retry_interval"=> 5),
                    //array('host'=>'127.0.0.1', 'port'=>8891, "timeout"=>"10", "persistent_id"=>"", "retry_interval"=> 5),
    ),
    "slave" => array(
                    array('host'=>'127.0.0.1', 'port'=>8889, "timeout"=>"10", "persistent_id"=>"", "retry_interval"=> 5),
                    //array('host'=>'127.0.0.1', 'port'=>8890, "timeout"=>"10", "persistent_id"=>"", "retry_interval"=> 5),
    )
);

try {
    
    $ssdb = new SsdbCluster($config, true, false);  // true 使用集群
    
    /*for ($i=0; $i<5; $i++){
       var_dump($ssdb->incr("hey"));
    }*/
    
    //sleep(1);
    //echo $ssdb->get("hey") . "\n";
    
    //echo $ssdb->set("key1", "hello world !", 100) . "\n"; 
    //sleep(1);
    
    //echo $ssdb->get("key1") . "\n"; 
    //echo $ssdb->del("key1") . "\n";
 
    //echo   $ssdb->count();
    
    
    //$ssdb->multi_set(array("key1" => 1, "key2" => 2, "key3" => 3));
    //var_dump($ssdb->multi_get(array("key1", "key2", "key3")));
    
    //var_dump($ssdb->multi_del(array("key2", "key3")));
    
    //var_dump($ssdb->hset("user_info", "user_id_1000", json_encode(array("name" => "雷锋", "age" => 18))));
    //var_dump($ssdb->hset("user_info", "user_id_1022", json_encode(array("name" => "阿东", "age" => 18))));
    /*sleep(1);
    var_dump($ssdb->hget("user_info", "user_id_1000"));
    var_dump($ssdb->hdel("user_info", "user_id_1000"));
    sleep(1);
    var_dump($ssdb->hget("user_info", "user_id_1000"));*/
    
   /* var_dump($ssdb->hincr("incr_hash", "rank", 10));
    var_dump($ssdb->hincr("incr_hash", "rank1", 1));
    sleep(1);
    var_dump($ssdb->hget("incr_hash", "rank"));
    var_dump($ssdb->hget("incr_hash", "rank1"));*/
    
    //var_dump($ssdb->hexists("incr_hash", "rank"));
    
    //var_dump($ssdb->hsize("incr_hash"));
    
    //var_dump($ssdb->hlist("", "", 1000));
   // var_dump($ssdb->hrlist("", "", 1000));
   
    //var_dump($ssdb->hkeys("user_info", '' ,'' ,10));
    
    /*var_dump($ssdb->hclear("user_info"));
    sleep(1);
    var_dump($ssdb->hgetall("user_info"));
    */
    
    /*$data = array(
            "user_id_100" => json_encode(array("name"=>"张山","age" => "10")),
            "user_id_1001" => json_encode(array("name"=>"李四","age" => "10")),
            "user_id_1002" => json_encode(array("name"=>"王五","age" => "15")),
            "user_id_1003" => json_encode(array("name"=>"赵六","age" => "17")),
            "user_id_1004" => json_encode(array("name"=>"孙悦","age" => "17")),
        
    );
    var_dump($ssdb->multi_hset("multi_user_info_1",$data));*/
    
    //var_dump($ssdb->hrscan("multi_user_info_1", '' , '', 100));
    //var_dump($ssdb->hgetall("multi_user_info_189"));
    
   /* $data = array(
        "user_id_100", 
        "user_id_1001", 
        "user_id_1002",
        "user_id_1003",
        "user_id_1004",
    );
    var_dump($ssdb->multi_hget("multi_user_info_1", $data));
    */
    
    /*$data = array(
        "user_id_100",
        "user_id_1001"
    );
    
    var_dump($ssdb->multi_hdel("multi_user_info_1", $data));*/
    
    //var_dump($ssdb->hgetall("multi_user_info"));
   //var_dump($ssdb->exists("key2"));
   // echo $ssdb->zset("key2", array("sdsdsd", "000777"));
   
    
   
    
    
} catch (Exception $e) {
    
    echo $e->getMessage();
    
}



 

 
