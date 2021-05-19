package screen_analyze.util
import redis.clients.jedis.Jedis
import redis.clients.jedis.JedisPool
import redis.clients.jedis.ShardedJedis
import redis.clients.jedis.ShardedJedisPool
import redis.clients.jedis.JedisPoolConfig
import redis.clients.jedis.JedisShardInfo
import java.util.ArrayList
import java.util.HashSet
import redis.clients.jedis.HostAndPort
import redis.clients.jedis.JedisCluster
import redis.clients.jedis.JedisSentinelPool
import java.util.Properties

object RedisUtil {
  val driverProps = new Properties()
    // 获取配置文件
    val driverConfig = getClass.getResourceAsStream("/driver.properties")
    driverProps.load(driverConfig)
  //redis
    val maxTotal = driverProps.getProperty("maxTotal")
    val maxIdle = driverProps.getProperty("maxIdle")
    val maxWaitMillis = driverProps.getProperty("maxWaitMillis")
    val redisPd = driverProps.getProperty("redispd");
    val redisip = driverProps.getProperty("redisip");
    val redisport = driverProps.getProperty("redisport");
    val keytimeout = driverProps.getProperty("keytimeout");
    val sentinel1 = driverProps.getProperty("sentinel1")
    val sentinel2 = driverProps.getProperty("sentinel2")
    val sentinel3 = driverProps.getProperty("sentinel3")
  // 非切片连接池
  var jedisPool: JedisPool = _
  // 哨兵连接池
  var jedisSentinelPool: JedisSentinelPool = _
  // 集群连接池
  var jedisCluster :JedisCluster = _
  initialPool()
//  var maxTotal:Int = 0
//  var maxIdle:Int = 0
//  var maxWaitMillis = 0L
//  var sentinel1:String = ""
//  var sentinel2:String = ""
//  var sentinel3:String = ""

  // 构造函数------
  // 初始化连接池
//  initialPool();
  // 初始化切片连接池
//  def this(maxTotal:Int,maxIdle:Int,maxWaitMillis:Long,sentinel1:String,sentinel2:String,sentinel3:String){
//    this()
//    this.maxIdle = maxIdle
//    this.maxWaitMillis = maxWaitMillis
//    this.maxTotal =maxTotal
//    this.sentinel1 = sentinel1
//    this.sentinel2 = sentinel2
//    this.sentinel3 = sentinel3
//    initialSentinePool();
//  }
  //redis集群
//  initialClusterPool();

  // 初始化非切片池
  def initialPool() {
    // 池的配置
    val config = new JedisPoolConfig();
    // 最大连接数
    config.setMaxTotal(maxTotal.toInt)
    // 最大空闲连接数
    config.setMaxIdle(maxIdle.toInt);
    //获取连接时的最大等待毫秒数
    config.setMaxWaitMillis(maxWaitMillis.toLong)
    //在空闲时检查有效性, 默认false
    config.setTestOnBorrow(false);
    jedisPool = new JedisPool(config, redisip, redisport.toInt,10000,redisPd);
  }

  // 初始化切片池(哨兵)
//  def initialSentinePool():JedisSentinelPool= {
//    // 池基本配置
//    val config = new JedisPoolConfig();
//    // 最大连接数
//    config.setMaxTotal(maxTotal.toInt)
//    // 最大空闲连接数
//    config.setMaxIdle(maxIdle.toInt);
//    //获取连接时的最大等待毫秒数
//    config.setMaxWaitMillis(maxWaitMillis.toLong)
//    // 在空闲时检查有效性,默认false
//    config.setTestOnBorrow(false);
//    //slave 连接
//    val shards = new HashSet[String]();
//    shards.add(sentinel1);
//    shards.add(sentinel2);
//    shards.add(sentinel3);
//    // 构造池
//     new JedisSentinelPool("mymaster",shards,config);  
//  }
  
  
  //redis集群
//  def initialClusterPool() {
//    val hostAndPortsSet = new HashSet[HostAndPort]();
//    // 添加节点
//    hostAndPortsSet.add(new HostAndPort("192.168.187.161", 7000));
//    hostAndPortsSet.add(new HostAndPort("192.168.187.161", 7001));
//    hostAndPortsSet.add(new HostAndPort("192.168.187.161", 7002));
//    hostAndPortsSet.add(new HostAndPort("192.168.187.161", 7003));
//    hostAndPortsSet.add(new HostAndPort("192.168.187.161", 7004));
//    hostAndPortsSet.add(new HostAndPort("192.168.187.161", 7005));
//
//    // Jedis连接池配置
//    val config = new JedisPoolConfig();
//    // 最大连接数
//    config.setMaxTotal(maxTotal.toInt)
//    // 最大空闲连接数
//    config.setMaxIdle(maxIdle.toInt);
//    //获取连接时的最大等待毫秒数
//    config.setMaxWaitMillis(maxWaitMillis.toLong)
//    // 在空闲时检查有效性,默认false
//    config.setTestOnBorrow(false);
//    jedisCluster = new JedisCluster(hostAndPortsSet, config);
//  }
}