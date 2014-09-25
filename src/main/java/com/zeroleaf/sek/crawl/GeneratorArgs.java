package com.zeroleaf.sek.crawl;

import com.zeroleaf.sek.util.SekArgs;

import org.kohsuke.args4j.Argument;

/**
 * @author zeroleaf
 */
public class GeneratorArgs extends SekArgs {

    @Argument(index = 1)
    private String appDir;

    public String getAppDir() {
        return appDir;
    }
}
