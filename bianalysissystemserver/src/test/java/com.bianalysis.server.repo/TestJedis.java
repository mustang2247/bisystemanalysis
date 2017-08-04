package com.bianalysis.server.repo;

import com.bianalysis.server.utils.PropUtil;
import redis.clients.jedis.Jedis;

/**
 * @Date Aug 6, 2015
 * @Author dengjie
 * @Note TODO
 */
public class TestJedis {

    public static void main(String[] args) {
        String envDir = PropUtil.getProp("/env.properties", "envdir");

        Jedis jedis = JedisFactory.getJedisInstance("/" + envDir + "redis.properties");
        jedis.set("20170804" + "_" + "jedis", "1");
        jedis.flushAll();
    }

}
