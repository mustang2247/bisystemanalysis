package com.bianalysis.server.conf;


import com.bianalysis.server.utils.PropUtil;

import java.util.Properties;

public class MysqlCfg {

    public String ADDR;
    public String DB_NAME;
    public String LOGIN_NAME;
    public String LOGIN_PASS;
    public int MAX_CONN;

    public boolean init( String filePath ) {

        Properties props = PropUtil.getProps( filePath );

        ADDR = props.getProperty( "addr" ).trim();
        DB_NAME = props.getProperty( "db_name" ).trim();
        LOGIN_NAME = props.getProperty( "login_name" ).trim();
        LOGIN_PASS = props.getProperty( "login_pass" ).trim();
        MAX_CONN = Integer.parseInt( props.getProperty( "max_conn" ).trim() );

        return true;
    }
}
