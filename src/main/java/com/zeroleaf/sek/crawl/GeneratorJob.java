package com.zeroleaf.sek.crawl;

import com.zeroleaf.sek.core.AbstractSJob;
import com.zeroleaf.sek.core.JobCreator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author zeroleaf
 */
public class GeneratorJob extends AbstractSJob {

    private static Logger LOGGER = LoggerFactory.getLogger(GeneratorJob.class);

    private final Path crawldb;
    private final Path seg;

    public GeneratorJob(Path crawldb, Path seg) {
        this.crawldb = crawldb;
        this.seg = seg;
    }

    private static class GeneratorMapper
        extends Mapper<Text, URLMeta, FetchEntry, NullWritable> {

        @Override
        protected void map(Text key, URLMeta value, Context context)
            throws IOException, InterruptedException {

            System.out.format("Generator Mapper - %nKey: %s, Value: %s%n",
                              key.toString(), value.toString());

            // 由于之前已经将 URL 进行去重处理了, 因此 FetchEntry 作为 Key 输出是可行的.
            context.write(new FetchEntry(key, value), NullWritable.get());
        }
    }

    private static class GeneratorReducer
        extends Reducer<FetchEntry, NullWritable, FetchEntry, NullWritable> {

        private int maxCount;

        @Override
        protected void setup(Context context)
            throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            maxCount = conf.getInt("sek.generate.max.count", Integer.MAX_VALUE);
            LOGGER.info("maxCount is {}", maxCount);
        }

        @Override
        protected void reduce(FetchEntry key, Iterable<NullWritable> values,
                              Context context)
            throws IOException, InterruptedException {

            if (maxCount-- > 0) {
                context.write(key, NullWritable.get());
            }
        }
    }

    @Override
    public Job create() throws IOException {

        JobCreator creator = new JobCreator();
        creator.addIn(crawldb).out(seg);

        creator.name(getName());

        creator.inputFormat(SequenceFileInputFormat.class);
        creator.mapper(GeneratorMapper.class);
        creator.reducer(GeneratorReducer.class);
        creator.outKey(FetchEntry.class);
        creator.outValue(NullWritable.class);

        return creator.get();
    }
}
