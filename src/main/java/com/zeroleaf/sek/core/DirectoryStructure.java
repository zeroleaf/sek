package com.zeroleaf.sek.core;

import org.apache.hadoop.fs.Path;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author zeroleaf
 */
public class DirectoryStructure {

    // CrawlDb
    private static final String CRAWL_DB = "crawldb";
    private static final String CRAWL_DB_CURRENT = "current";
    private static final String CRAWL_DB_OLD = "old";

    // Segments
    private static final String SEGMENTS = "segments";
    private static final String FETCH_LIST = "fetchlist";

    private final Path base;

    // CrawlDb
    private Path crawldb0;
    private Path crawldbCurrent;
    private Path crawldbOld;

    // Segments
    private Path segments;

    // 当前的 segment
    private Path segment;

    private Path fetchlist;

    private static Map<String, DirectoryStructure> bases = new HashMap<>();

    private DirectoryStructure(String path) {
        this.base = new Path(path);

        crawldb0 = new Path(base, CRAWL_DB);
        segments = new Path(base, SEGMENTS);
    }


    // @TODO 如何能更好的实现并发?
    /**
     * 
     * @param appDir
     * @return
     */
    public synchronized static DirectoryStructure get(String appDir) {
        DirectoryStructure dSt = bases.get(appDir);
        if (dSt == null) {
            dSt = new DirectoryStructure(appDir);
            bases.put(appDir, dSt);
        }
        return dSt;
    }

    public Path getCrawlDb() {
        if (crawldbCurrent == null) {
            crawldbCurrent = new Path(crawldb0, CRAWL_DB_CURRENT);
        }
        return crawldbCurrent;
    }

    public Path getCrawldbOld() {
        if (crawldbOld == null) {
            crawldbOld = new Path(crawldb0, CRAWL_DB_OLD);
        }
        return crawldbOld;
    }

    public Path getSegment() {
        return segment;
    }

    public Path newSegment() {
        segment = new Path(segments, newSegmentName());
        return segment;
    }

    private static String newSegmentName() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        return sdf.format(new Date(System.currentTimeMillis()));
    }

    public Path getFetchlist() {
        if (segment == null) {
            newSegment();
        }
        if (fetchlist == null) {
            fetchlist = new Path(segment, FETCH_LIST);
        }
        return fetchlist;
    }


}
