package com.client;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

public class PropertiesCfg {
    private final static String file = "myproperties.properties";
    private final static Properties properties = new Properties();

    static {
        try {
            properties.load(new InputStreamReader(ClassLoader.getSystemResourceAsStream(file), "utf-8"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //根据key获取值
    public static String get(String key) {
        return properties.getProperty(key).trim();
    }

    //根据key获取值，值为空则返回defaultValue
    public static String get(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }
}
