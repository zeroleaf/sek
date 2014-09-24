package com.zeroleaf.sek.crawl;

import com.zeroleaf.sek.util.SekArgs;

import org.kohsuke.args4j.Argument;

/**
 * Injector 命令的命令行参数.
 */
class InjectorArgs extends SekArgs {

    @Argument(index = 1)
    private String appDir;

    @Argument(index = 2)
    private String urlsDir;

    public String getAppDir() {
        return appDir;
    }

    public String getUrlsDir() {
        return urlsDir;
    }
}
