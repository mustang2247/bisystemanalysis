package com.bianalysis.data;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.bianalysis.server.conf.FieldNames;
import com.bianalysis.server.redis.RedisManager;
import com.bianalysis.server.repo.TestMysql;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class TestData {
    private static final Logger logger = LoggerFactory.getLogger(TestMysql.class);

    /**
     * install 测试数据
     */
    @Test
    public void testInstallData() {
        try {
            LocalDateTime date = LocalDateTime.now();

            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");
            System.out.println(date.format(dateTimeFormatter));// new Date()为获取当前系统时间

            Map<String, String> map;
            Jedis jedis = RedisManager.getJedis();
            jedis.del(FieldNames.STREAM_INSTALL);

            for (int i = 0; i < 10; i++) {
                map = new HashMap<String, String>();
//                map.put("appid", (i + 1) + "");
                map.put("deviceid", "deviceid_" + i);
                map.put("createtime", dateTimeFormatter.format(date));
                map.put("updatetime", dateTimeFormatter.format(date));
                map.put("date", dateFormatter.format(date));
                map.put("time", timeFormatter.format(date));
                map.put("idfa", "");
                map.put("idfv", "");
                map.put("channelid", "");

                JSONObject jsonObject = new JSONObject();
                jsonObject.put("appid", (i + 1) + "");
                jsonObject.put("context", JSON.toJSONString(map));

                jedis.lpush(FieldNames.STREAM_INSTALL, jsonObject.toJSONString());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testStartupData() {
        try {
            LocalDateTime date = LocalDateTime.now();

            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");

            Map<String, String> map;
            Jedis jedis = RedisManager.getJedis();
            jedis.del(FieldNames.STREAM_STARTUP);

            for (int i = 0; i < 10; i++) {
                map = new HashMap<String, String>();
//                map.put("appid", (i + 1) + "");
                map.put("deviceid", "deviceid_" + i);
                map.put("createtime", dateTimeFormatter.format(date));
                map.put("updatetime", dateTimeFormatter.format(date));
                map.put("date", dateFormatter.format(date));
                map.put("time", timeFormatter.format(date));
                map.put("idfa", "");
                map.put("idfv", "");
                map.put("channelid", "");
                map.put("ip", "localhost");
                map.put("network", "wifi");
                map.put("devicetype", "ios");
                map.put("os", "ios 10");
                map.put("op", "china");
                map.put("resolution", "1080*720");
                map.put("tz", "北京");

                JSONObject jsonObject = new JSONObject();
                jsonObject.put("appid", (i + 1) + "");
                jsonObject.put("context", JSON.toJSONString(map));

                jedis.lpush(FieldNames.STREAM_STARTUP, jsonObject.toJSONString());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getData() {

        Long len = RedisManager.getJedis().llen(FieldNames.STREAM_INSTALL);
        while (len > 0) {
            String content = RedisManager.getJedis().rpop(FieldNames.STREAM_INSTALL);
            logger.info(content);
            len--;
        }

    }
}
