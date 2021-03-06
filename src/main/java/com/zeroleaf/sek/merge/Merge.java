package com.zeroleaf.sek.merge;

import com.zeroleaf.sek.core.AbstractCommand;
import com.zeroleaf.sek.core.DirectoryStructure;
import com.zeroleaf.sek.crawl.CrawlDb;
import com.zeroleaf.sek.util.FileSystems;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zeroleaf on 14-12-3.
 */
public class Merge extends AbstractCommand {

    private static Logger LOGGER = LoggerFactory.getLogger(Merge.class);

    @Override
    public String getName() {
        return "merge";
    }

    @Override
    public void showUsage() {
        HelpFormatter hf = new HelpFormatter();
        hf.printHelp("merge <appdir>", mergeArgs);
    }

    @Override
    public void execute(String... args) throws Exception {
        try {
            LOGGER.info("正在执行任务: {}", getName());

            if (args.length < 2)
                throw new IllegalArgumentException("<appdir> 必须提供");

            String appdir = args[1];

            DirectoryStructure dSt = DirectoryStructure.get(appdir);

            // 合并 DataDb 数据l
            DataDb datadb = new DataDb(appdir);
            datadb.merge(dSt.getParsedata());

            // 合并 CrawlDb 数据
            Path rOut = randomDirectory();
            runJob(new NewUrlJob(dSt.getParsedata(), rOut));

            CrawlDb crawlDb = new CrawlDb(appdir);
            crawlDb.merge(false, rOut);

            FileSystems.deleteDirectory(rOut);

            LOGGER.info("任务完成: {}", getName());

        } catch (Exception e) {
            e.printStackTrace();
            showUsage();
        }
    }

    private static Options mergeArgs = new Options();
    static {
    }

    public static void main(String[] args) throws Exception {
        args = new String[]{"merge", "sek"};
        new Merge().execute(args);
    }
}
