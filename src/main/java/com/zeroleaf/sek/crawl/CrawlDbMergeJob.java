package com.zeroleaf.sek.crawl;

import com.zeroleaf.sek.core.AbstractSJob;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Set;

/**
 * @author zeroleaf
 */
public class CrawlDbMergeJob extends AbstractSJob {

    private Set<Path> inputPaths;
    private Path outputPath;

    public CrawlDbMergeJob(Set<Path> inputPaths, Path outputPath) {
        this.inputPaths = inputPaths;
        this.outputPath = outputPath;
    }

    private static class CrawlDbMergeMapper
        extends Mapper<Text, URLMeta, Text, URLMeta> {

        @Override
        protected void map(Text key, URLMeta value, Context context)
            throws IOException, InterruptedException {
            super.map(key, value, context);
        }
    }

    @Override
    public Job create() throws IOException {
        return null;
    }
}
