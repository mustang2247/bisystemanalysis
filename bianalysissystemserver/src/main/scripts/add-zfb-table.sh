#! /bin/bash -

DB_ADDR=localhost
DB_PORT=3306
LOGIN_NAME='root'
LOGIN_PASS='hoolai-game'
DB_NAME='BiServer'

# add tables
mysql -h${DB_ADDR} -P${DB_PORT} -u${LOGIN_NAME} -p${LOGIN_PASS} -D${DB_NAME} -e \
"CREATE TABLE t_zfb_bet (
  id int(11) NOT NULL AUTO_INCREMENT,
  timestamp bigint(20) NOT NULL,
  appid varchar(32) NOT NULL,
  chips bigint(20) NOT NULL,
  user_id bigint(20) NOT NULL,
  user_info varchar(256) NOT NULL,
  inning_id varchar(32) NOT NULL,
  banker_id bigint(20) NOT NULL,
  is_join char(1) NOT NULL,
  PRIMARY KEY (id),
  KEY timestamp (timestamp)
);";

mysql -h${DB_ADDR} -P${DB_PORT} -u${LOGIN_NAME} -p${LOGIN_PASS} -D${DB_NAME} -e \
"CREATE TABLE t_zfb_win (
  id int(11) NOT NULL AUTO_INCREMENT,
  timestamp bigint(20) NOT NULL,
  appid varchar(32) NOT NULL,
  chips bigint(20) NOT NULL,
  user_id bigint(20) NOT NULL,
  user_info varchar(256) NOT NULL,
  inning_id varchar(32) NOT NULL,
  banker_id bigint(20) NOT NULL,
  is_join char(1) NOT NULL,
  PRIMARY KEY (id),
  KEY timestamp (timestamp)
);";

mysql -h${DB_ADDR} -P${DB_PORT} -u${LOGIN_NAME} -p${LOGIN_PASS} -D${DB_NAME} -e \
"CREATE TABLE t_zfb_pump (
  id int(11) NOT NULL AUTO_INCREMENT,
  timestamp bigint(20) NOT NULL,
  appid varchar(32) NOT NULL,
  chips bigint(20) NOT NULL,
  user_id bigint(20) NOT NULL,
  user_info varchar(256) NOT NULL,
  inning_id varchar(32) NOT NULL,
  banker_id bigint(20) NOT NULL,
  is_join char(1) NOT NULL,
  PRIMARY KEY (id),
  KEY timestamp (timestamp)
);";

mysql -h${DB_ADDR} -P${DB_PORT} -u${LOGIN_NAME} -p${LOGIN_PASS} -D${DB_NAME} -e \
"CREATE TABLE t_zfb_jackpot (
  id int(11) NOT NULL AUTO_INCREMENT,
  timestamp bigint(20) NOT NULL,
  appid varchar(32) NOT NULL,
  chips bigint(20) NOT NULL,
  user_id bigint(20) NOT NULL,
  user_info varchar(256) NOT NULL,
  inning_id varchar(32) NOT NULL,
  banker_id bigint(20) NOT NULL,
  is_join char(1) NOT NULL,
  PRIMARY KEY (id),
  KEY timestamp (timestamp)
);";
