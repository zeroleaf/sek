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

    @Override
    public Job create() throws IOException {
        JobCreator creator = new JobCreator();
        creator.addIn(in);
        creator.out(out);

        creator.mapper(InjectorMapper.class);
        creator.outKey(Text.class);
        creator.outValue(URLMeta.class);
        creator.name(getName());

        return creator.get();
    }
}
