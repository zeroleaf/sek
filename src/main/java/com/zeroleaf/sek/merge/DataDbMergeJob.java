package com.zeroleaf.sek.merge;

import com.zeroleaf.sek.core.AbstractSJob;
import com.zeroleaf.sek.core.JobCreator;
import com.zeroleaf.sek.parse.ParsedEntry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;

import java.io.IOException;
import java.util.*;

/**
 * Created by zeroleaf on 14-12-6.
 */
public class DataDbMergeJob extends AbstractSJob {

    private Set<Path> ins = new HashSet<>();
    private Path out;

    public DataDbMergeJob(Path out, Path in, Path ... others) {
        this.out = out;
        this.ins.add(in);
        Collections.addAll(this.ins, others);
    }

    private static class DataDbMergeJobMapper
            extends Mapper<Text, ParsedEntry, Text, ParsedEntry> {

        @Override
        protected void map(Text key, ParsedEntry value, Context context)
                throws IOException, InterruptedException {

            context.write(key, value);
        }
    }

    private static class DataDbMergeJobReducer
            extends Reducer<Text, ParsedEntry, Text, ParsedEntry> {

        @Override
        protected void reduce(Text key, Iterable<ParsedEntry> values, Context context)
                throws IOException, InterruptedException {

            // 如果有多个 URL 解析结果, 我门选择最近访问的那个.
            context.write(key, getSortedLastElement(values, ParsedEntry.MODIFIED_TIME_COMPARATOR));
        }

        private static ParsedEntry getSortedLastElement(Iterable<ParsedEntry> iter, Comparator<ParsedEntry> cmp) {
//            List<T> list = CollectionUtils.iterToList(iter);
            List<ParsedEntry> list = new ArrayList<>();
            for (ParsedEntry entry : iter)
                list.add(entry.clone());

            if (list.size() == 1)
                return list.get(0);

            Collections.sort(list, cmp);
            return list.get(list.size() - 1);
        }
    }

    @Override
    public Job create() throws IOException {
        JobCreator jobCreator = new JobCreator();

        jobCreator.addIns(ins);
        jobCreator.out(out);

        jobCreator.inputFormat(SequenceFileInputFormat.class);
        jobCreator.mapper(DataDbMergeJobMapper.class);
        jobCreator.reducer(DataDbMergeJobReducer.class);
        jobCreator.outKey(Text.class);
        jobCreator.outValue(ParsedEntry.class);
        jobCreator.outFormat(MapFileOutputFormat.class);

        return jobCreator.get();
    }
}
