package com.zeroleaf.sek.download;

import com.zeroleaf.sek.core.AbstractSJob;
import com.zeroleaf.sek.core.JobCreator;
import com.zeroleaf.sek.crawl.FetchEntry;
import com.zeroleaf.sek.data.PageEntry;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;

import java.io.IOException;

/**
 * Created by zeroleaf on 14-12-1.
 */
public class DownloaderJob extends AbstractSJob {

    private Path fetchlist;
    private Path fetchdata;

    public DownloaderJob(Path fetchlist, Path fetchdata) {
        this.fetchlist = fetchlist;
        this.fetchdata = fetchdata;
    }

    @Override
    public Job create() throws IOException {
        JobCreator jobCreator = new JobCreator();

        jobCreator.name(getName());

        jobCreator.addIn(fetchlist);
        jobCreator.out(fetchdata);

//        jobCreator.inputFormat(SequenceFileInputFormat.class);
        jobCreator.mapper(DownloaderMapper.class);
        jobCreator.outKey(Text.class);
        jobCreator.outValue(PageEntry.class);
//        jobCreator.outFormat(MapFileOutputFormat.class);

        return jobCreator.get();
    }
}
