package com.bianalysis.server.conf;

import java.util.Properties;

public class BiGateCfg extends ClientConfig {

    public String LOGIN_NAME;
    public String LOGIN_PASS;
    public int MESSAGE_VERSION;
    public int IO_THREAD_NUM;
    public int LOGIC_THREAD_NUM;

    @Override
    public boolean extraInit( Properties props ) {

        LOGIN_NAME = props.getProperty( "login_name" ).trim();
        LOGIN_PASS = props.getProperty( "login_pass" ).trim();
        MESSAGE_VERSION = Integer.parseInt( props.getProperty( "message_version" ).trim() );
        IO_THREAD_NUM = Integer.parseInt( props.getProperty( "io_thread_num" ).trim() );
        LOGIC_THREAD_NUM = Integer.parseInt( props.getProperty( "logic_thread_num" ).trim() );

        return true;
    }
}
