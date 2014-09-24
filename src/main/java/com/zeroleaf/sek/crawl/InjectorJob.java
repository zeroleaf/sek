package com.zeroleaf.sek.crawl;

import com.zeroleaf.sek.core.AbstractSJob;
import com.zeroleaf.sek.core.JobCreator;
import com.zeroleaf.sek.core.URLFilters;
import com.zeroleaf.sek.core.URLNormalizers;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author zeroleaf
 */
public class InjectorJob extends AbstractSJob {

    private static Logger LOGGER = LoggerFactory.getLogger(InjectorJob.class);

    private final Path in;
    private final Path out;

    public InjectorJob(Path in, Path out) {
        this.in = in;
        this.out = out;
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

    /**
     * @TODO Reducer 类, 目前为简单实现. 即去重, 只保留一个. 目前的实现貌似是取最后一个.
     * 在实际中, 可能要根据具体策略决定留下哪个.
     */
    public static class InjectorReducer
        extends Reducer<Text, URLMeta, Text, URLMeta> {

        @Override
        protected void reduce(Text key, Iterable<URLMeta> values, Context context)
            throws IOException, InterruptedException {

            URLMeta first = getFirst(values);
            if (first != null) {
                context.write(key, first);
            }
        }

        private static <T> T getFirst(Iterable<T> iterable) {
            for (T t : iterable)
                return t;
            return null;
        }
    }

    @Override
    public Job create() throws IOException {
        JobCreator creator = new JobCreator();
        creator.addIn(in);
        creator.out(out);

        creator.name(getName());
        creator.mapper(InjectorMapper.class);
        creator.reducer(InjectorReducer.class);
        creator.outKey(Text.class);
        creator.outValue(URLMeta.class);

        return creator.get();
    }
}
