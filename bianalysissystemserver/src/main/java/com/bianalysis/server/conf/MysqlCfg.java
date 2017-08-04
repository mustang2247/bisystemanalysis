package com.bianalysis.server.conf;


import com.bianalysis.server.utils.PropUtil;

import java.util.Properties;

/**
 * mysql 配置文件
 */
public class MysqlCfg {

    public String ADDR;         //连接地址
    public String DB_NAME;      //数据库名字
    public String LOGIN_NAME;   //登陆名
    public String LOGIN_PASS;   //登陆密码
    public int MAX_CONN;        //最大链接数

    public boolean init(String filePath ) {

        Properties props = PropUtil.getProps( filePath );

        ADDR = props.getProperty( "addr" ).trim();
        DB_NAME = props.getProperty( "db_name" ).trim();
        LOGIN_NAME = props.getProperty( "login_name" ).trim();
        LOGIN_PASS = props.getProperty( "login_pass" ).trim();
        MAX_CONN = Integer.parseInt( props.getProperty( "max_conn" ).trim() );

        return true;
    }
}
