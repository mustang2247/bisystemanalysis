#! /bin/bash -

DB_ADDR=1.2.51.161
DB_PORT=1033
LOGIN_NAME=root
LOGIN_PASS=xiangxing!@#
DB_NAME=BiServer
OP_NAME=biserver
OP_PASS='Vluty-a4apk)Iw4r'

mysql -h${DB_ADDR} -P${DB_PORT} -u${LOGIN_NAME} -p${LOGIN_PASS} -e "CREATE DATABASE ${DB_NAME};";
mysql -h${DB_ADDR} -P${DB_PORT} -u${LOGIN_NAME} -p${LOGIN_PASS} -e "CREATE USER '${OP_NAME}'@'%' IDENTIFIED BY '${OP_PASS}';";
mysql -h${DB_ADDR} -P${DB_PORT} -u${LOGIN_NAME} -p${LOGIN_PASS} -e "GRANT ALL PRIVILEGES ON ${DB_NAME}.* TO '${OP_NAME}'@'%';";

# add tables
mysql -h${DB_ADDR} -P${DB_PORT} -u${LOGIN_NAME} -p${LOGIN_PASS} -D${DB_NAME} -e \
	"CREATE TABLE t_install ( id INT NOT NULL AUTO_INCREMENT,
	timestamp BIGINT NOT NULL,
	appid VARCHAR(32) NOT NULL,
	user_id BIGINT NOT NULL,
	open_id VARCHAR(32) NOT NULL,
	inviter_id VARCHAR(32) NOT NULL,
	from_where VARCHAR(128) NOT NULL,
	ip VARCHAR(40) NOT NULL,
	remark VARCHAR(256) NOT NULL,
	PRIMARY KEY (id),
	INDEX(timestamp) );";

mysql -h${DB_ADDR} -P${DB_PORT} -u${LOGIN_NAME} -p${LOGIN_PASS} -D${DB_NAME} -e \
	"CREATE TABLE t_dau ( id INT NOT NULL AUTO_INCREMENT,
	timestamp BIGINT NOT NULL,
	appid VARCHAR(32) NOT NULL,
	user_id BIGINT NOT NULL,
	ip VARCHAR(40) NOT NULL,
	platform VARCHAR(16) NOT NULL,
	creative VARCHAR(64) NOT NULL,
	market_info VARCHAR(128) NOT NULL,
	PRIMARY KEY (id),
	INDEX(timestamp) );";

mysql -h${DB_ADDR} -P${DB_PORT} -u${LOGIN_NAME} -p${LOGIN_PASS} -D${DB_NAME} -e \
	"CREATE TABLE t_pay ( id INT NOT NULL AUTO_INCREMENT,
	timestamp BIGINT NOT NULL,
	appid VARCHAR(32) NOT NULL,
	user_id BIGINT NOT NULL,
	user_info VARCHAR(256) NOT NULL,
	item_id INT NOT NULL,
	item_buy_count INT NOT NULL,
	item_count INT NOT NULL,
	platform VARCHAR(16) NOT NULL,
	order_info VARCHAR(128) NOT NULL,
	money BIGINT NOT NULL,
	money2 BIGINT NOT NULL,
	money3 BIGINT NOT NULL,
	ip VARCHAR(40) NOT NULL,
	PRIMARY KEY (id),
	INDEX(timestamp) );";


	

