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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * @author zeroleaf
 */
public class CrawlDbMergeJob extends AbstractSJob {

    private static Logger LOGGER = LoggerFactory.getLogger(CrawlDbMergeJob.class);

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
     * 去重, 并合并所有元数据生成一个新的, 完整的元数据.
     */
    private static class CrawlDbMergeReducer
        extends Reducer<Text, URLMeta, Text, URLMeta> {

        @Override
        protected void reduce(Text key, Iterable<URLMeta> values,
                              Context context)
            throws IOException, InterruptedException {

            URLMeta value = merge(values);
            LOGGER.debug("{} 合并之后的元数据为 {}", key, value);

            context.write(key, value);
        }

        /**
         * 合并同一 URL 对应的元数据.
         *
         * 以最后访问的为模板, 同时会将元数据设置为最近一个的元数据.
         *
         * @param metas URL 对应的元数据.
         * @return 合并之后的元数据.
         */
        private static URLMeta merge(Iterable<URLMeta> metas) {
            List<URLMeta> list = iterToList(metas);
            if (list.size() == 1)
                return list.get(0);

            Collections.sort(list, URLMeta.MODIFIED_TIME_COMPARATOR);
            URLMeta template = list.get(list.size() - 1);
            boolean changed = false;
            for (int i = list.size() - 2; i >= 0; i--) {
                URLMeta tmp = list.get(i);

                if (template.getScore() != tmp.getScore()) {
                    template.setScore(tmp.getScore());
                    changed = true;
                }

                if (template.getFetchInterval() != tmp.getFetchInterval()) {
                    template.setFetchInterval(tmp.getFetchInterval());
                    changed = true;
                }

                if (changed)
                    break;
            }

            return template;
        }

        private static <T> List<T> iterToList(Iterable<T> iter) {
            List<T> list = new ArrayList<>();
            for (T t : iter)
                list.add(t);
            return list;
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
