package com.zeroleaf.sek.crawl;

import com.zeroleaf.sek.core.AbstractSJob;
import com.zeroleaf.sek.core.JobCreator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

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
        extends Mapper<Text, URLMeta, Text, URLMeta> {

        @Override
        protected void map(Text key, URLMeta value, Context context)
            throws IOException, InterruptedException {

            System.out.format("Key: %s, Value: %s%n",
                              key.toString(), value.toString());

            context.write(key, value);
        }
    }

    /**
     * @TODO 目前为简单实现. 去重.
     */
    private static class CrawlDbMergeReducer
        extends Reducer<Text, URLMeta, Text, URLMeta> {

        @Override
        protected void reduce(Text key, Iterable<URLMeta> values,
                              Context context)
            throws IOException, InterruptedException {

            URLMeta value = getFirst(values);
            System.out.format("Merge Reducer - Key: %s, Value: %s%n",
                              key.toString(), value.toString());
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
        creator.inputFormat(SequenceFileInputFormat.class);
        creator.mapper(CrawlDbMergeMapper.class);
        creator.reducer(CrawlDbMergeReducer.class);
        creator.outKey(Text.class);
        creator.outValue(URLMeta.class);
        creator.outFormat(MapFileOutputFormat.class);

        return creator.get();
    }
}
