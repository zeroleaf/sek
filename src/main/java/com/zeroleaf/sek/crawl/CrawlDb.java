package com.zeroleaf.sek.crawl;

import com.zeroleaf.sek.core.DirectoryStructure;
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

    private DirectoryStructure dSt;

    public CrawlDb(String appDir) {
        dSt = DirectoryStructure.get(appDir);
    }

    public void install(Path src) throws IOException {
        moveToLocalFile(src, dSt.getCrawlDb());
    }

    /**
     * 判断是否有 CrawlDb 存在.
     *
     * 特别的, 如果存在 crawldb/current 文件夹, 即认为 CrawlDb 存在.
     *
     * @return 如果 CrawlDb 存在, 返回 true; 否则, false.
     * @throws IOException
     */
    public boolean exist() throws IOException {
        return exists(dSt.getCrawlDb());
    }

    /**
     * 指定路径下的数据与 CrawlDb 的数据进行合并.
     *
     * 参数 reverse 决定是否备份之前的 CrawlDb 数据.
     *
     * @param reverse 在合并前是否备份原来的 CrawlDb 数据.
     * @param ins 包含有要合并的数据的路径.
     *
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public void merge(boolean reverse, Path... ins)
        throws IOException, ClassNotFoundException, InterruptedException {

        Path outputPath = randomPath();
        Job job = new CrawlDbMergeJob(outputPath, dSt.getCrawlDb(), ins).create();

        job.waitForCompletion(true);

        if (reverse) {
            deleteDirectory(dSt.getCrawldbOld());
            moveToLocalFile(dSt.getCrawlDb(), dSt.getCrawldbOld());
        } else {
            deleteDirectory(dSt.getCrawlDb());
        }
        moveToLocalFile(outputPath, dSt.getCrawlDb());
    }

    private static Path randomPath() {
        return randomDirectory("crawldb-");
    }
}
