/**
 * Author: guanxin
 * Date: 2015-07-24
 */

package com.bianalysis.server.conf;

import com.bianalysis.server.utils.PropUtil;

import java.util.Properties;

/**
 * redis config
 */
public class RedisConfig {

    public int REDIS_PORT;

    public String REDIS_HOST;

    public String jedisName;

    public boolean init(String filePath) {
        return init(PropUtil.getProps(filePath));
    }

    public boolean init(Properties props) {
        REDIS_HOST = props.getProperty("redisHost").trim();
        jedisName = props.getProperty("jedisName").trim();
        REDIS_PORT = Integer.parseInt(props.getProperty("redisPort").trim());
        return extraInit(props);
    }

    public boolean extraInit(Properties props) {
        return true;
    }
}
