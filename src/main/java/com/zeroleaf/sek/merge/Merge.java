package com.zeroleaf.sek.merge;

import com.zeroleaf.sek.core.AbstractCommand;
import com.zeroleaf.sek.core.DirectoryStructure;
import com.zeroleaf.sek.crawl.CrawlDb;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.Path;

/**
 * Created by zeroleaf on 14-12-3.
 */
public class Merge extends AbstractCommand {

    @Override
    public String getName() {
        return "merge";
    }

    @Override
    public void showUsage() {

    }

    @Override
    public void execute(String... args) throws Exception {
        try {
            if (args.length < 2)
                throw new IllegalArgumentException("<appdir> bixu ");

            String appdir = args[1];

            DirectoryStructure dSt = DirectoryStructure.get(appdir);
            Path rOut = randomDirectory();
            runJob(new NewUrlJob(dSt.getParsedata(), rOut));

            CrawlDb crawlDb = new CrawlDb(appdir);

//            crawlDb.merge(rOut);



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
