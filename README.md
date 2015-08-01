##SsdbCluster
    SsdbCluster 是一个依托于phpssdb（https://github.com/jonnywang/phpssdb/ php的ssdb扩展）构建ssdb客户端类


##维护人
* huangjialin  https://github.com/huangjialin


##特色

    1) 多主多从，随机获取主从对象
    2) 对象实例化时候检查服务器是否可用
    3）待续

##demo
eg：      
 $config = array(
    "master" => array(
                    array('host'=>'127.0.0.1','port'=>8888),
    ),
    "slave" => array(
                    array('host'=>'127.0.0.1','port'=>8889),
                    array('host'=>'127.0.0.1','port'=>8890),
    )
);
try {
    
    $ssdb = new SsdbCluster($config,true);  // true 使用组从 false 不使用主从
    for ($i=0; $i<1000; $i++){
        var_dump($ssdb->incr("hey"));
    }
    echo $ssdb->get("hey") . "\n";
    
} catch (Exception $e) {
    echo $e->getMessage();
}

##相关扩展

phpssdb https://github.com/jonnywang/phpssdb/
