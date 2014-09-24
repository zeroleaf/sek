package com.zeroleaf.sek.core;

import com.zeroleaf.sek.SekConf;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *
 * @author zeroleaf
 */
public class JobCreator {

    private Job job;

    public JobCreator() throws IOException {
        job = Job.getInstance(SekConf.getInstance());
    }

    public JobCreator addIn(String in) throws IOException {
        return addIn(new Path(in));
    }

    public JobCreator addIn(Path in) throws IOException {
        FileInputFormat.addInputPath(job, in);
        return this;
    }

    public JobCreator out(String out) {
        return this.out(new Path(out));
    }

    public JobCreator out(Path out) {
        FileOutputFormat.setOutputPath(job, out);
        return this;
    }

    public JobCreator name(String name) {
        job.setJobName(name);
        return this;
    }

    public JobCreator mapper(Class<? extends Mapper> mapperClass) {
        job.setMapperClass(mapperClass);
        return this;
    }

    public JobCreator reducer(Class<? extends Reducer> reducerClass) {
        job.setReducerClass(reducerClass);
        return this;
    }

    public JobCreator outKey(Class<?> keyClass) {
        job.setOutputKeyClass(keyClass);
        return this;
    }

    public JobCreator outValue(Class<?> valueClass) {
        job.setOutputValueClass(valueClass);
        return this;
    }

    public Job get() {
        return job;
    }
}
