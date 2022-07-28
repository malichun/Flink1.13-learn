package com.atguigu.flume.interceptor;

import com.alibaba.fastjson.JSONObject;

/**
 * @author malichun
 * @create 2022/07/27 0027 23:48
 */
public class JSONUtils {
    public static boolean isJSON(String log){
        boolean flag = false;
        // 判断log是否是json
        try {
            JSONObject.parseObject(log);
            flag = true;
        }catch (com.alibaba.fastjson.JSONException e){

        }
        return flag;
    }
}
