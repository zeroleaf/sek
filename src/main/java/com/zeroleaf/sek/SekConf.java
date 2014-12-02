package com.zeroleaf.sek;

import com.zeroleaf.sek.util.ConfEntry;

import org.apache.hadoop.conf.Configuration;

/**
 * @author zeroleaf
 */
public class SekConf extends Configuration {

    static {
        addDefaultResource("sek-default.xml");
    }

    private static SekConf sekConf = new SekConf();

    public SekConf() {
        super();
    }

    public static SekConf getInstance() {
        return sekConf;
    }

    public static boolean getBoolean(ConfEntry<Boolean> entry) {
        return sekConf.getBoolean(entry.getName(), entry.getValue());
    }

    public static int getInt(String key) {
        return sekConf.getInt(key, 0);
    }
}
