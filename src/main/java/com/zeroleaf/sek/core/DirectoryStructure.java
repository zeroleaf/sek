package com.zeroleaf.sek.core;

import org.apache.hadoop.fs.Path;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Sek 数据输出的目录结构.
 *
 * 首先是一个 base 目录, 所有的输出都在该文件夹下.
 * 特别的, 有如下的几个子文件夹:
 *
 * * crawldb - 保存 URL 数据的文件夹.
 * ** current - 当前的 URL 数据, 是从这里获取数据生成 fetchlist 的.
 * ** old - 上一次的 URL 数据, 备份.
 * * segments - 里面包含有多个 segment.
 * ** segment - 一次抓取对应一个 segment. 命名规则为 年月日时分秒.
 * *** fetchlist - 此次要抓取的 URL 列表.
 *
 *
 * 通过静态工厂方法 get(String) 传入 base 路径来获取该类的实例.
 * 由于创建多个相同 base 的实例没有多大意义,
 * 因此 get() 对于同一 base 总是返回相同的实例.
 *
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
    private static final String FETCH_DATA = "fetchdata";

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
    private Path fetchdata;

    private static Map<String, DirectoryStructure> bases = new HashMap<>();

    private DirectoryStructure(String path) {
        this.base = new Path(path);

        crawldb0 = new Path(base, CRAWL_DB);
        segments = new Path(base, SEGMENTS);
    }


    // @TODO 如何能更好的实现并发?
    /**
     * 
     * @param base
     * @return
     */
    public synchronized static DirectoryStructure get(String base) {
        DirectoryStructure dSt = bases.get(base);
        if (dSt == null) {
            dSt = new DirectoryStructure(base);
            bases.put(base, dSt);
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

    public Path getFetchlist(boolean create) {
        if (segment == null) {
            if (create)
                newSegment();
            else
                segment = findLastSegment();
        }
        if (segment == null)
            return null;

        return new Path(segment, FETCH_LIST);
//        if (fetchlist == null) {
//            if (!create)
//                return null;
//            fetchlist = new Path(segment, FETCH_LIST);
//        }
//        return fetchlist;
    }

    public Path getFetchdata() {
        if (segment == null)
            segment = findLastSegment();

        return new Path(segment, FETCH_DATA);
    }

    public Path findLastSegment() {

        String[] dirs = new File(segments.toString()).list();
        if (dirs.length == 0)
            return null;

        Arrays.sort(dirs);
        return new Path(segments, dirs[dirs.length - 1]);
    }

    public Path findLastFetchlist() {
        Path seg = findLastSegment();
        if (seg == null)
            return null;

        // 该方法不会创建文件夹.
        return new Path(seg, FETCH_LIST);
    }

    public boolean exists(Path hp) {
        if (hp == null)
            return false;
        return Files.exists(Paths.get(hp.toString()));
    }

}
