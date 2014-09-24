package com.zeroleaf.sek.crawl;

import com.zeroleaf.sek.SekConf;
import com.zeroleaf.sek.util.ConfEntry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @author zeroleaf
 */
public class CrawlDb {

    public static final ConfEntry<Boolean> REVERSE =
        new ConfEntry<>("sek.crawldb.reverse", true);

    private static final String CRAWL_DB = "crawldb";

    private static final String CURRENT = "current";

    private static final String OLD = "old";

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

    public void install(Path src) throws IOException {
        FileSystem fs = FileSystem.get(new SekConf());
        fs.moveToLocalFile(src, getStoragePath());
    }

    public void merge(Path... paths) {
        final boolean reverse = SekConf.getBoolean(REVERSE);
    }
}
