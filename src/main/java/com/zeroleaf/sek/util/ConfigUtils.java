package com.zeroleaf.sek.util;

import org.apache.hadoop.conf.Configuration;

/**
 * Created by zeroleaf on 14-12-3.
 *
 * 读取配置的辅助类.
 *
 * 配置可能有 2 个来源:
 *
 * 1. 系统属性
 * 2. 配置文件
 *
 * 其中系统属性是由命令行参数设置的, 因此其优先级最高.
 * 该类的所有方法都先尝试读取系统属性, 如果有则返回.
 * 如果没有对应的系统属性, 则从配置文件中获取, 同时默认配置文件中一定存在对应的参数.
 * 否则, 返回的是对应类型的默认值.
 *
 * 在使用任何方法之前必先调用 {setConf()} 设置对应的配置, 不然调用其他方法是可能抛
 * {IllegalStateException} 异常.
 *
 * 同时注意, 该类<em>不是线程安全的</em>.
 */
public class ConfigUtils {

    private ConfigUtils() {}

    private static Configuration conf;

    public static void setConf(Configuration config) {
        conf = config;
    }

    public static int getInt(String key) {
        String val = System.getProperty(key);
        if (val != null) {
            try {
                return Integer.valueOf(val);
            } catch (NumberFormatException e) {
                // 忽略
            }
        }
        if (conf == null)
            throw new IllegalStateException("在读取配置内容前必先设置.");
        return conf.getInt(key, -1);
    }
}
