package com.zeroleaf.sek.crawl;

import org.apache.hadoop.fs.Path;

/**
 * @author zeroleaf
 */
public class CrawlDb {

    private static final String CRAWL_DB = "crawlDb";

    private static final String CURRENT = "current";

    public static Path getCrawlDbPath(String appDir) {
        return new Path(appDir, CRAWL_DB);
    }

    public static Path getStoragePath(String appDir) {
        return new Path(getCrawlDbPath(appDir), CURRENT);
    }

    public static void merge(Path ... paths) {

    }

    public static void install(Path path) {

    }
}
