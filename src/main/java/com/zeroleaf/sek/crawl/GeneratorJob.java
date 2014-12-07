package com.zeroleaf.sek.crawl;

import com.zeroleaf.sek.core.AbstractSJob;
import com.zeroleaf.sek.core.JobCreator;

import jdk.nashorn.internal.runtime.regexp.joni.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author zeroleaf
 *
 * @TODO 要忽略以访问过的 URL.
 */
public class GeneratorJob extends AbstractSJob {

    private static Logger LOGGER = LoggerFactory.getLogger(GeneratorJob.class);

    private static final String MAX_COUNT = "sek.generate.max.count";

    private final Path crawldb;
    private final Path seg;

    public GeneratorJob(Path crawldb, Path seg) {
        this.crawldb = crawldb;
        this.seg = seg;
    }

    private static class GeneratorMapper
        extends Mapper<Text, URLMeta, LongWritable, FetchEntry> {

        private int maxCount;
        private int index = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            maxCount = conf.getInt(MAX_COUNT, Integer.MAX_VALUE);
            if (maxCount != Integer.MAX_VALUE)
                LOGGER.info("种子URL数量限制为 {}", maxCount);

        }

        @Override
        protected void map(Text key, URLMeta value, Context context)
            throws IOException, InterruptedException {

            LOGGER.info("{} => {}", key, value);


            if (index < maxCount && needCrawl(key, value)) {
                context.write(new LongWritable(index++), new FetchEntry(key, value));
            }

//            System.out.format("Generator Mapper - %nKey: %s, Value: %s%n",
//                              key.toString(), value.toString());

            // 由于之前已经将 URL 进行去重处理了, 因此 FetchEntry 作为 Key 输出是可行的.
//            context.write(new FetchEntry(key, value), NullWritable.get());
        }

        private boolean needCrawl(Text url, URLMeta meta) {
            if (meta.getStatus() == URLMeta.Status.FETCHED) {
                long now = System.currentTimeMillis();
                boolean isNeed = (now - meta.getModifiedTime()) > (meta.getFetchInterval() * 1000);
                if (!isNeed)
                    LOGGER.info("URL {}, 其元数据为 {}, 当前不需要再次抓取, 忽略.", url, meta);
            }

            // 对于其他情况, 目前我们简单的返回true.
            return true;
        }
    }

//    private static class GeneratorReducer
//        extends Reducer<FetchEntry, NullWritable, FetchEntry, NullWritable> {
//
//        private int maxCount;
//
//        @Override
//        protected void setup(Context context)
//            throws IOException, InterruptedException {
//
//            Configuration conf = context.getConfiguration();
//            maxCount = conf.getInt("sek.generate.max.count", Integer.MAX_VALUE);
//            LOGGER.info("maxCount is {}", maxCount);
//        }
//
//        @Override
//        protected void reduce(FetchEntry key, Iterable<NullWritable> values,
//                              Context context)
//            throws IOException, InterruptedException {
//
//            LOGGER.debug("maxCount = {}, FetchEntry is {}", maxCount, key);
//
//            if (maxCount-- > 0) {
//                context.write(key, NullWritable.get());
//            }
//        }
//    }

    @Override
    public Job create() throws IOException {

        JobCreator creator = new JobCreator();
        creator.addIn(crawldb).out(seg);

        creator.name(getName());

        creator.inputFormat(SequenceFileInputFormat.class);
        creator.mapper(GeneratorMapper.class);
//        creator.reducer(GeneratorReducer.class);
        creator.outKey(LongWritable.class);
        creator.outValue(FetchEntry.class);
        creator.outFormat(MapFileOutputFormat.class);

        return creator.get();
    }
}
