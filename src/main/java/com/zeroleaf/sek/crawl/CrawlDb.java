package com.zeroleaf.sek.crawl;

import com.zeroleaf.sek.SekConf;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @author zeroleaf
 */
public class CrawlDb {

    private static final String CRAWL_DB = "crawldb";

    private static final String CURRENT = "current";

    private final String appDir;

    public CrawlDb(String appDir) {
        this.appDir = appDir;
    }

    public Path getCrawlDbPath() {
        return new Path(appDir, CRAWL_DB);
    }

    public Path getStoragePath() {
        return new Path(getCrawlDbPath(), CURRENT);
    }

    public void merge(Path ... paths) {

    }

    public void install(Path path) throws IOException {
        FileSystem fs = FileSystem.get(new SekConf());
        fs.moveToLocalFile(path, getStoragePath());
    }
}
