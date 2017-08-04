package com.bianalysis.server.repo;

import com.bianalysis.server.sql.SqlCommand;
import com.bianalysis.server.sql.SqlCommandRegistry;
import com.bianalysis.server.utils.PropUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

/**
 * 测试 mysql
 */
public class TestMysql {

    private static final Logger logger = LoggerFactory.getLogger(TestMysql.class);

    @Test
    public void testSql() {
        String envDir = PropUtil.getProp("/env.properties", "envdir");


        try {
            // sql 命令初始化
            SqlCommandRegistry.getInstance().init("/" + envDir + "sql.xml");

            // init mysql datasource
            RepoManager.init("/" + envDir + "mysql.properties");

            BiRepo biRepo = RepoManager.getBiRepo();

            LocalDateTime date = LocalDateTime.now();

            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");
            System.out.println(date.format(dateTimeFormatter));// new Date()为获取当前系统时间

            // 入库
            SqlCommand cmd = SqlCommandRegistry.getInstance().get("install");
            String[] values;
            for (int i = 0; i < 10; i++) {
                values = new String[]{(i + 1) + "", "deviceid_" + i, dateTimeFormatter.format(date), dateTimeFormatter.format(date), dateFormatter.format(date), timeFormatter.format(date), "", "", ""};
                try {
                    if (!cmd.push(values)) {
                        logger.error("Fail report op:{} args: {}", "install", Arrays.asList(values));
                    } else {
                    }
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
