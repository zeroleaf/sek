package com.zeroleaf.sek.merge;

import com.zeroleaf.sek.core.*;
import com.zeroleaf.sek.crawl.URLMeta;
import com.zeroleaf.sek.parse.Outlink;
import com.zeroleaf.sek.parse.ParsedEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by zeroleaf on 14-12-3.
 *
 * 从解析的数据提取出新的 URL.
 */
public class NewUrlJob extends AbstractSJob {

    private static Logger LOGGER = LoggerFactory.getLogger(NewUrlJob.class);

    private Path parsedata;
    private Path out;

    public NewUrlJob(Path parsedata, Path out) {
        this.parsedata = parsedata;
        this.out       = out;
    }

    private static class NewUrlJobMapper
            extends Mapper<Text, ParsedEntry, Text, URLMeta> {

        private URLNormalizerAggregator urlNormalizer = new URLNormalizerAggregator();
        private URLFilterAggregator     urlFilter     = new URLFilterAggregator();

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();

            if (conf.getBoolean("sek.merge.normalizer.parameter", true)) {
                urlNormalizer.addURLNormalizer(MarkNormalizer.PARAMETER_NORMALIZER);
            }

            if (conf.getBoolean("sek.merge.normalizer.anchor", true)) {
                urlNormalizer.addURLNormalizer(MarkNormalizer.ANCHOR_NORMALIZER);
            }

            if (conf.getBoolean("sek.merge.filter.http", true)) {
                urlFilter.addURLFilter(new HttpFilter());
            }
        }

        @Override
        protected void map(Text key, ParsedEntry value, Context context)
                throws IOException, InterruptedException {

            URLMeta meta = metaFromParsedEntry(value);
            LOGGER.debug("URL {} 的新元数据为 {}", key, meta);
            context.write(key, meta);

            Set<Outlink> outlinks = value.getOutlinks();
            for (Outlink outlink : outlinks) {

                String url = outlink.getToUrl();

                url = urlNormalizer.normalize(url);
                if (urlFilter.filter(url))
                    continue;

                LOGGER.debug("新URL {}, 来自 {}", url, key);
                context.write(new Text(url), URLMeta.newDefaultUnfetched());
            }
        }

        private static URLMeta metaFromParsedEntry(ParsedEntry entry) {
            URLMeta meta = new URLMeta();
            meta.setStatus(URLMeta.Status.FETCHED);
            meta.setModifiedTime(entry.getModifiedTime());
            return meta;
        }
    }

    private static class NewUrlJobReducer
            extends Reducer<Text, URLMeta, Text, URLMeta> {

        @Override
        protected void reduce(Text key, Iterable<URLMeta> values, Context context)
                throws IOException, InterruptedException {

            context.write(key, getFirst(values));
        }

        private static <T> T getFirst(Iterable<T> iterable) {
            Iterator<T> iterator = iterable.iterator();
            if (iterator.hasNext())
                return iterator.next();
            return null;
        }
    }

    @Override
    public Job create() throws IOException {
        JobCreator jobCreator = new JobCreator();
        jobCreator.name(getName());

        jobCreator.addIn(parsedata);
        jobCreator.out(out);

        jobCreator.inputFormat(SequenceFileInputFormat.class);
        jobCreator.mapper(NewUrlJobMapper.class);
        jobCreator.reducer(NewUrlJobReducer.class);
        jobCreator.outKey(Text.class);
        jobCreator.outValue(URLMeta.class);
        jobCreator.outFormat(MapFileOutputFormat.class);

        return jobCreator.get();
    }
}
