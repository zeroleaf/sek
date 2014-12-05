package com.zeroleaf.sek.parse;

import com.zeroleaf.sek.core.AbstractSJob;
import com.zeroleaf.sek.core.JobCreator;
import com.zeroleaf.sek.data.PageEntry;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by zeroleaf on 14-12-3.
 */
public class ParserJob extends AbstractSJob {

    private static Logger LOGGER = LoggerFactory.getLogger(ParserJob.class);

    private Path fetchdata;
    private Path parsedata;

    public ParserJob(Path fetchdata, Path parsedata) {
        this.fetchdata = fetchdata;
        this.parsedata = parsedata;
    }

    private static class ParserMapper
            extends Mapper<Text, PageEntry, Text, ParsedEntry> {

        private static Pattern PAT_A_TAG = Pattern.compile("<a.*?href=\"(.*?)\".*?>(.*?)</a>");

        @Override
        protected void map(Text key, PageEntry value, Context context)
                throws IOException, InterruptedException {

            context.write(key, parse(value));
        }

        private static ParsedEntry parse(PageEntry page) {

            ParsedEntry parsedEntry = ParsedEntry.valueOf(page);

            String html = page.getContent().toString();
            LOGGER.debug("网页内容 {}", html);
            Matcher matcher = PAT_A_TAG.matcher(html);
            while (matcher.find()) {
                Outlink outlink = new Outlink(matcher.group(1), matcher.group(2));
                parsedEntry.addOutlink(outlink);
            }

            return parsedEntry;
        }
    }

    @Override
    public Job create() throws IOException {
        JobCreator jobCreator = new JobCreator();

        jobCreator.name(getName());

        jobCreator.addIn(fetchdata);
        jobCreator.out(parsedata);

        jobCreator.inputFormat(SequenceFileInputFormat.class);
        jobCreator.mapper(ParserMapper.class);
        jobCreator.outKey(Text.class);
        jobCreator.outValue(ParsedEntry.class);
        jobCreator.outFormat(MapFileOutputFormat.class);

        return jobCreator.get();
    }
}
