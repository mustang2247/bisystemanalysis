package com.bianalysis.server.protocol;

public class CommandList {

	public static final int ERROR = -1;
	public static final int IGNORE = -2;

	public static final int LOGIN_FROM_GAME_SERVER = -11;
	public static final int LOGIN_FROM_BI_SERVER = -12;
	public static final int LOGIN_FROM_GM = -13;
	public static final int LOGOUT_FROM_BI_SERVER = -14;

	public static final int GET_GM_USER_LIST = -20;
	public static final int NEW_GM_USER = -21;
	public static final int DEL_GM_USER = -22;
	public static final int GET_GM_USER_AUTH = -23;
	public static final int SET_GM_USER_AUTH = -24;
	public static final int MOD_GM_USER_PASS = -25;

	public static final int NEW_BI_SERVER_USER = -31;
	public static final int DEL_BI_SERVER_USER = -32;
	public static final int NEW_GAME_SERVER_USER = -33;
	public static final int DEL_GAME_SERVER_USER = -34;
	public static final int GET_BI_SERVER_USER_LIST = -35;
	public static final int GET_GAME_SERVER_USER_LIST = -36;

	public static final int REPORT = 1;
	public static final int ERROR_FROM_BI = 2;

	public static final int UPDATE_DATA = 89;
	public static final int GET_LONG = 90;
	public static final int GET_STRING = 91;
	public static final int GET_LIST_LONG = 92;
	public static final int GET_LIST_STRING = 93;
	public static final int GET_MAP_LONG_LONG = 94;
	public static final int GET_MAP_LONG_STRING = 95;
	public static final int GET_MAP_STRING_LONG = 96;


	public static final int GET_USER_INSTALL_RETENTION = 99;
	public static final int GET_SUMMARY_INSTALL_PAY = 100;
	public static final int GET_USER_INSTALL_PAY_DAYS = 101;
	public static final int GET_SINGLE_USER_FINANCE = 102;
	public static final int GET_SINGLE_USER_ENEMY = 103;
	public static final int GET_SINGLE_USER_IP = 104;
	public static final int GET_SINGLE_USER_POSSIBLE_IP = 105;
	public static final int GET_SINGLE_USER_PAY = 106;
	public static final int GET_USER_ALL_ONLINE = 107;

	public static final int GET_SUMMARY_FINANCE = 110;

	public static final int GET_ZFB_ROBOT = 1000;
	public static final int GET_ZFB_BASIC = 1001;
	public static final int GET_ZFB_RANK = 1002;
	public static final int GET_ZFB_RED_ENVELOPE = 1003;

	public static final int GET_MTL_BASIC = 2001;
	public static final int GET_MTL_RANK = 2002;

	public static final int GET_MTL2_FINANCE = 2010;
	public static final int GET_MTL2_RANK = 2011;

	public static final int GET_FRUIT_BASIC = 3001;
	public static final int GET_FRUIT_CAIBEI_BASIC = 3002;
	public static final int GET_FRUIT_CAIBEI_ITEM = 3003;
	public static final int GET_FRUIT_RANK = 3004;

	public static final int GET_PPT_BASIC = 4001;
	public static final int GET_PPT_SSC_BASIC = 4002;
	public static final int GET_PPT_RANK = 4003;

	public static final int GET_DOGSPORT_BASIC = 5001;
	public static final int GET_DOGSPORT_RANK = 5002;

	public static final int GET_FARM_BASIC = 6001;

	public static final int GET_TEXASPOKER_RANK = 7001;
	public static final int GET_TEXASPOKER_YOULUN = 7002;
	public static final int GET_TEXASPOKER_PAY_DETAIL = 7003;

	public static final int GET_NEWPOKER_JICHU = 7004;
	public static final int GET_NEWPOKER_ONLINE = 7005;
	public static final int GET_NEWPOKER_SHOUSHU = 7006;


	public static final int GET_SCMJ_BASIC = 8001;
	public static final int GET_SCMJ_RANK = 8002;

	public static final int GET_RONGLIAN_SUMMARY = 9001;

	public static final int GET_MP_COST = 10001;

	public static final int GET_MJDR_FINANCE = 11001;
	public static final int GET_MJDR_FUNNEL = 11002;
	public static final int GET_MJDR_ONLINE = 11003;
	public static final int GET_MJDR_UPGRADE = 11005;
	public static final int GET_MJDR_MISSION = 11006;
	public static final int GET_MJDR_BASIC = 11008;
	public static final int GET_MJDR_STAT = 11009;
	public static final int GET_MJDR_RANK = 11010;
	public static final int GET_MJDR_POSSESSION = 11011;
	public static final int GET_MJDR_PAY_ITEM = 11012;
	public static final int GET_MJDR_HAND1_RETENTION = 11013;
	public static final int GET_MJDR_LOTTERY_STAT = 11014;
	public static final int GET_MJDR_LOTTERY_ITEM = 11015;
	public static final int GET_MJDR_LOTTERY_SUM = 11016;
	public static final int GET_MJDR_AI_WINLOSE = 11017;
	public static final int GET_MJDR_PHONE_CARD_RETENTION = 11018;
	public static final int GET_MJDR_LIANXI = 11019;
	public static final int GET_MJDR_LIANXIWIN = 11020;
	public static final int GET_MJDR_HUAFEIGIFT = 11021;

	public static final int GET_PRODUCT_LOGIN_GIFT = 12001;
	public static final int GET_PRODUCT_LOGIN_PHONE_CARD = 12002;
	public static final int GET_PRODUCT_LUCKY_FUND = 12003;
	public static final int GET_PRODUCT_EXCHANGE_BASIC = 12004;
	public static final int GET_PRODUCT_MONTH_GIFT = 12005;

	public static final int GET_ACTIVITY_GOLDEGG = 20001;

	public static final int GET_YELLOW_GIFT = 30001;

	public static final int GET_PROPS_ZONGLIANG= 40001;
	public static final int GET_PROPS_SEND= 40002;
	public static final int GET_PROPS_HUAFEI= 40003;
	public static final int GET_PROPS_1HUAFEI= 40004;

	public static final int GET_DDZ_JICHU = 50003;
	public static final int GET_DDZ_TASK = 50004;
	public static final int GET_DDZ_MULT = 50005;
	public static final int GET_DDZ_CHOUJIANG = 50006;
	public static final int GET_DDZ_LUCKY_WHEEL = 50007;

}
