package com.bianalysis.server.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.util.List;

/**
 * JSON工具类
 */
public class JSONUtils
{

    /***
     * 将List对象序列化为JSON文本
     */
    public static <T> String toJSONString(List<T> list)
    {
        return JSON.toJSONString(list);
    }

    /***
     * 将对象序列化为JSON文本
     * @param object
     * @return
     */
    public static String toJSONString(Object object)
    {
        return JSON.toJSONString(object);
    }

    /**
     * 将json转换为对象
     * @param text
     * @return
     */
    public static Object toObject(String text)
    {
        return JSON.parse(text);
    }

    /**
     * 将json转换为对象
     *
     * @param text
     * @return
     */
    public static JSONObject toJSONObject(String text) {
        return JSON.parseObject(text);
    }


}
