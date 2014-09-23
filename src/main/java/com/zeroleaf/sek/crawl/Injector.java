package com.zeroleaf.sek.crawl;

import com.zeroleaf.sek.core.AbstractCommand;
import com.zeroleaf.sek.core.CommandException;

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
 * 1. 读取种子URL, 根据规则进行过滤.
 * 2. 如果原先不存在db, 则新建; 存在则merge.
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
    public void execute(String... args) throws CommandException {

    }

    public void inject(String storageDir, String urlsDir)
        throws IOException, ClassNotFoundException, InterruptedException {
        Path storage = new Path(storageDir);
        Path urls = new Path(urlsDir);

        Job job = createJob();

        job.setMapperClass(InjectorMapper.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, urls);
        FileOutputFormat.setOutputPath(job, storage);

        job.waitForCompletion(true);
    }

    public static class InjectorMapper
        extends Mapper<LongWritable, Text, LongWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

            String meta = value.toString().trim();
            if (meta.isEmpty()) {
                return;
            }

            SeedUrl url;
            try {
                url = SeedUrl.parse(meta);
            } catch (Exception e) {
                LOGGER.warn("Invalid seed url found: {}", meta);
                return;
            }

            context.write(key, new Text(url.toString()));
        }
    }

    public static void main(String[] args)
        throws InterruptedException, IOException, ClassNotFoundException {
        Injector injector = new Injector();
        injector.inject("sek", "urls");

//        System.out.println(new SekConf().get("sek.injector.seed"));
    }
}
