package com.zeroleaf.sek.merge;

import com.zeroleaf.sek.core.AbstractSJob;
import com.zeroleaf.sek.core.JobCreator;
import com.zeroleaf.sek.crawl.URLMeta;
import com.zeroleaf.sek.parse.Outlink;
import com.zeroleaf.sek.parse.ParsedEntry;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import java.io.IOException;
import java.util.Set;

/**
 * Created by zeroleaf on 14-12-3.
 *
 * 从解析的数据提取出新的 URL.
 */
public class NewUrlJob extends AbstractSJob {

    private Path parsedata;
    private Path out;

    public NewUrlJob(Path parsedata, Path out) {
        this.parsedata = parsedata;
        this.out       = out;
    }

    private static class NewUrlJobMapper
            extends Mapper<Text, ParsedEntry, Text, URLMeta> {

        @Override
        protected void map(Text key, ParsedEntry value, Context context)
                throws IOException, InterruptedException {

            URLMeta meta = metaFromParsedEntry(value);
            context.write(key, meta);

            Set<Outlink> outlinks = value.getOutlinks();
            for (Outlink outlink : outlinks)
                context.write(new Text(outlink.getToUrl()), URLMeta.newDefaultUnfetched());
        }

        private static URLMeta metaFromParsedEntry(ParsedEntry entry) {
            URLMeta meta = new URLMeta();
            meta.setStatus(URLMeta.Status.FETCHED);
            meta.setModifiedTime(entry.getModifiedTime());
            return meta;
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
        jobCreator.outKey(Text.class);
        jobCreator.outValue(URLMeta.class);

        return jobCreator.get();
    }
}
