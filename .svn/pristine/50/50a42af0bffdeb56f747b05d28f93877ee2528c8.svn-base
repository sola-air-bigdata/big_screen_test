package screen_analyze.service.analyze_bill
import com.alibaba.fastjson.JSONObject
import org.apache.log4j.Logger
import screen_analyze.util.RedisUtil
import redis.clients.jedis.Jedis

class StationShowMbTableUpdate {
  @transient private lazy val log = Logger.getLogger(this.getClass)
  var jedisutil: Jedis = _
  var valueTimeout:Int = 0
  def this(jedisutil: Jedis,valueTimeout:Int) {
    this()
    this.jedisutil = jedisutil
    this.valueTimeout = valueTimeout
  }
  def analyzeUpdateTable(jsonSql: JSONObject) {
    val jedis = jedisutil
    try {
      //     log.info("********StationShowMbTableUpdate处理json:" + jsonSql.toJSONString())
      val dataArr = jsonSql.getJSONArray("data")
      val oldArr = jsonSql.getJSONArray("old")
      var arrSize = dataArr.size()
      var old = new JSONObject
      var data = new JSONObject
      for (m <- 0 to arrSize - 1) {
        data = dataArr.getJSONObject(m)
        old = oldArr.getJSONObject(m)
        val l_member_id = data.getLongValue("l_member_id")
        val l_station_id = data.getLongValue("l_station_id")
        val keys = old.keySet()
        val itr = keys.iterator()
        while(itr.hasNext()){
          var s = itr.next()
          var key = "SMBT"+"_"+l_member_id+"_"+l_station_id+"_"+s
          var value = data.getLongValue(s)
          val isexist = jedis.exists(key)
          if(isexist){
            jedis.setex(key, valueTimeout, value+"")
          }
        }
      }
      jedis.close()
    } catch {
      case ex: Exception => log.info("*****StationShowMbTableUpdate**redis处理异常****")
    } finally {
      if(jedis!=null){
        jedis.close()
      }
    }
  }
}