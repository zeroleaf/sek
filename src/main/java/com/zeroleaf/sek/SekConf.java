package com.zeroleaf.sek;

import org.apache.hadoop.conf.Configuration;

/**
 * @author zeroleaf
 */
public class SekConf extends Configuration {

    static {
        addDefaultResource("sek-default.xml");
    }

    public SekConf() {
        super();
    }
}
