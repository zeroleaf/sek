package com.zeroleaf.sek.crawl;

import com.zeroleaf.sek.SekConf;
import com.zeroleaf.sek.core.AbstractCommand;

import com.zeroleaf.sek.core.SekException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 *
 * @author zeroleaf
 */
public class Injector extends AbstractCommand {

    private static Logger LOGGER = LoggerFactory.getLogger(Injector.class);

    private InjectorArgs args = new InjectorArgs();

    @Override
    public String getName() {
        return "inject";
    }

    @Override
    public void execute(String... as) throws Exception {
        try {

            parseArgs(args, as);

            checkArgs();

            inject(args.getAppDir(), args.getUrlsDir());
        } catch (Exception e) {
            // @TODO 出错原因更加细化.
            existCode = 10;
            throw e;
        }
    }

    private void checkArgs() throws SekException {
        if (Files.notExists(Paths.get(args.getUrlsDir()))) {
            throw new SekException("url are required.");
        }
    }


    public void inject(String appDir, String urlsDir)
        throws IOException, ClassNotFoundException, InterruptedException {

        final Path tmpOut = runInjectJob(urlsDir);
        FileSystem fs = FileSystem.get(new SekConf());
        CrawlDb crawlDb = new CrawlDb(appDir);

        try {
            if (crawlDb.exist()) {
                crawlDb.merge(tmpOut);
            } else {
                crawlDb.install(tmpOut);
            }
        } finally {
            if (fs.exists(tmpOut)) {
                fs.delete(tmpOut, true);
            }
        }
    }

    private Path runInjectJob(String urls)
        throws IOException, InterruptedException, ClassNotFoundException {
        Path out = randomDirectory();

        runJob(new InjectorJob(new Path(urls), out));

        return out;
    }

    @Override
    protected void doAfterSuccess(Job job) throws IOException {
        LOGGER.info("Injector: number of filtered url - {} ",
                    job.getCounters().findCounter("injector", "url_filtered")
                        .getValue());
    }
}
