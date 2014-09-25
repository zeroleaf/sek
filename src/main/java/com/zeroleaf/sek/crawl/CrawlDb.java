package com.zeroleaf.sek.crawl;

import com.zeroleaf.sek.SekConf;
import com.zeroleaf.sek.util.ConfEntry;

import static com.zeroleaf.sek.util.FileSystems.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

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

    public Path getOldStoragePath() {
        return new Path(getCrawlDbPath(), OLD);
    }

    public void install(Path src) throws IOException {
        moveToLocalFile(src, getStoragePath());
    }

    public void merge(Path... paths)
        throws IOException, ClassNotFoundException, InterruptedException {
        Path outputPath = randomPath();
        Job job = new CrawlDbMergeJob(outputPath, getStoragePath(), paths).create();

        job.waitForCompletion(true);

        final boolean reverse = SekConf.getBoolean(REVERSE);
        if (reverse) {
            deleteDirectory(getOldStoragePath());
            moveToLocalFile(getStoragePath(), getOldStoragePath());
        } else {
            deleteDirectory(getStoragePath());
        }
        moveToLocalFile(outputPath, getStoragePath());
    }

    private static Path randomPath() {
        return randomDirectory("crawldb-");
    }
}
