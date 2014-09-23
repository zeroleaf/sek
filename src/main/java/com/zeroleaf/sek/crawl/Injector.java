package com.zeroleaf.sek.crawl;

import com.zeroleaf.sek.SekConf;
import com.zeroleaf.sek.core.AbstractCommand;
import com.zeroleaf.sek.core.URLFilters;
import com.zeroleaf.sek.core.URLNormalizers;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 1. 读取种子URL, 根据规则进行过滤. 2. 如果原先不存在db, 则新建; 存在则merge.
 *
 * @author zeroleaf
 */
public class Injector extends AbstractCommand {

    private static Logger LOGGER = LoggerFactory.getLogger(Injector.class);

    @Override
    public String getName() {
        return "inject";
    }

    @Override
    public void execute(String... args) throws Exception {
        final String appDir = args[0];
        final String urlsDir = args[1];
        inject(appDir, urlsDir);
    }

    public void inject(String appDir, String urlsDir)
        throws IOException, ClassNotFoundException, InterruptedException {

        final Path tmpOut = injectUrls(urlsDir);
        FileSystem fs = FileSystem.get(new SekConf());
        CrawlDb crawlDb = new CrawlDb(appDir);

        try {
            if (fs.exists(crawlDb.getStoragePath())) {
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

    private Path injectUrls(String urlsDir)
        throws IOException, ClassNotFoundException, InterruptedException {
        Path urls = new Path(urlsDir);
        Path tmp = randomPath();

        Job job = createJob();

        job.setMapperClass(InjectorMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(URLMeta.class);

        FileInputFormat.addInputPath(job, urls);
        FileOutputFormat.setOutputPath(job, tmp);

        job.waitForCompletion(true);

        LOGGER.info("Injector: number of filtered url - {} ",
                    job.getCounters().findCounter("injector", "url_filtered").getValue());

        return tmp;
    }

    public static class InjectorMapper
        extends Mapper<LongWritable, Text, Text, URLMeta> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

            String meta = value.toString().trim();
            if (meta.isEmpty()) {
                return;
            }

            SeedUrl seedUrl;
            try {
                seedUrl = SeedUrl.parse(meta);
            } catch (Exception e) {
                LOGGER.warn("Invalid seed url found: {}", meta);
                return;
            }

            final String url = URLNormalizers.normalize(seedUrl.getUrl());
            if (url.isEmpty()) {
                return;
            }

            if (URLFilters.filter(seedUrl.getUrl())) {
                context.getCounter("injector", "url_filtered").increment(1);
            }

            context.write(new Text(url), seedUrl.toURLMeta());
        }
    }

    public static void main(String[] args)
        throws InterruptedException, IOException, ClassNotFoundException {
        Injector injector = new Injector();
        injector.inject("sek", "urls");
    }
}
