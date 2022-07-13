package utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

/**
 * 在维度置换的时候, 查询hbase -> phoenix 维度信息, 填充dwm层的数据
 * @author malichun
 * @create 2022/07/12 0012 23:42
 */
public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws Exception {
        // 查询Phoenix之前先查询Redis
        Jedis jedis = RedisUtil.getJedis();
        // 1.存储数据类型: jsonStr
        // 2.使用什么类型: String
        // 3.RedisKey:  tableName+id
        String redisKey = "DIM:"+tableName+":"+id;
        String dimInfoJsonStr = jedis.get(redisKey);
        if(dimInfoJsonStr != null){
            // 归还连接
            jedis.close();
            // 重置过期时间
            jedis.expire(redisKey, 24 * 60 * 60);
            // 返回结果
            return JSON.parseObject(dimInfoJsonStr);
        }

        // 拼接SQL查询语句
        // select * from db.tn where id = '18';
        String querySql = "select * from "+ GmallConfig.HBASE_SCHEMA + "." + tableName
            + " where id ='" + id + "'";

        // 查询Phoenix
        List<JSONObject> queryList = JDBCUtil.queryList(connection, querySql, JSONObject.class, false);

        JSONObject dimInfoJson = queryList.get(0);
        // 在返回结果之前, 将数据写入redis
        jedis.set(redisKey, dimInfoJson.toJSONString());
        jedis.expire(redisKey, 24 * 60 * 60);
        jedis.close();
        // 返回结果
        return dimInfoJson;
    }

    public static void delRedisDimInfo(String tableName, String id){
        Jedis jedis = RedisUtil.getJedis();
        String redisKey = "DIM:"+tableName+":"+id;
        jedis.del(redisKey);
        jedis.close();
    }

    public static void main(String[] args) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        long start = System.currentTimeMillis();
        System.out.println(getDimInfo(connection, "DIM_BASE_TRADEMARK", "15"));
        //long end = System.currentTimeMillis();
        //System.out.println(getDimInfo(connection, "DIM_BASE_TRADE_MARK", "15"));
        //long end2 = System.currentTimeMillis();
        //System.out.println(getDimInfo(connection, "DIM_BASE_TRADE_MARK", "15"));
        //long end3 = System.currentTimeMillis();
        //System.out.println(end - start);
        //System.out.println(end2 - end);
        //System.out.println(end3 - end2);
        connection.close();;
    }
}
