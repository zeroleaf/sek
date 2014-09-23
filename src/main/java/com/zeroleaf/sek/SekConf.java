package com.zeroleaf.sek;

import org.apache.hadoop.conf.Configuration;

/**
 * @author zeroleaf
 */
public class SekConf extends Configuration {

    static {
        addDefaultResource("sek-default.xml");
    }

    static SekConf sekConf = new SekConf();

    public SekConf() {
        super();
    }

    public static SekConf getInstance() {
        return sekConf;
    }
}
