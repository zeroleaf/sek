package com.zeroleaf.sek.crawl;

import com.zeroleaf.sek.core.AbstractSJob;
import com.zeroleaf.sek.core.JobCreator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

/**
 * @author zeroleaf
 */
public class CrawlDbMergeJob extends AbstractSJob {

    private Set<Path> inputPaths = new HashSet<>();
    private Path outputPath;

    public CrawlDbMergeJob(Path outputPath, Path first, Path... others) {
        inputPaths.add(first);
        Collections.addAll(inputPaths, others);
        this.outputPath = outputPath;
    }

    private static class CrawlDbMergeMapper
        extends Mapper<Text, Text, Text, Text> {

        @Override
        protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

            System.out.format("Key: %s, Value: %s",
                              key.toString(), value.toString());

            context.write(key, value);
        }
    }

    /**
     * @TODO 目前为简单实现. 去重.
     */
    private static class CrawlDbMergeReducer
        extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values,
                              Context context)
            throws IOException, InterruptedException {

            Text value = getFirst(values);
            if (value != null) {
                context.write(key, value);
            }
        }

        private static <T> T getFirst(Iterable<T> iterable) {
            for (T t : iterable) {
                return t;
            }
            return null;
        }
    }

    @Override
    public Job create() throws IOException {
        JobCreator creator = new JobCreator();
        creator.addIns(inputPaths);
        creator.out(outputPath);

        creator.name(getName());
        creator.inputFormat(KeyValueTextInputFormat.class);
        creator.mapper(CrawlDbMergeMapper.class);
        creator.reducer(CrawlDbMergeReducer.class);
        creator.outKey(Text.class);
        creator.outValue(Text.class);

        return creator.get();
    }

    public static void main(String[] args)
        throws IOException, ClassNotFoundException, InterruptedException {
        Path out = new Path("merge-" + UUID.randomUUID().toString());
        Path first = new Path("tes/crawldb/current");
//        Path second = new Path("nek/crawldb/current");
        Job job = new CrawlDbMergeJob(out, first).create();

        job.waitForCompletion(true);
    }
}
