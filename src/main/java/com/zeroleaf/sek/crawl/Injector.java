package com.zeroleaf.sek.crawl;

import com.zeroleaf.sek.AbstractCommand;
import com.zeroleaf.sek.CommandException;
import com.zeroleaf.sek.SekConf;

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

            System.out.format("Key is %s, Value is %s%n", key.toString(), value.toString());
            context.write(key, value);
        }
    }

    public static void main(String[] args)
        throws InterruptedException, IOException, ClassNotFoundException {
//        Injector injector = new Injector();
//        injector.inject("sek", "/tmp/urls");

        System.out.println(new SekConf().get("sek.injector.seed"));
    }
}
