package com.zeroleaf.sek.core;

import com.zeroleaf.sek.SekConf;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Collection;

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

    public JobCreator addIns(Collection<Path> ins) throws IOException {
        for (Path in : ins) {
            addIn(in);
        }
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

    public JobCreator outFormat(Class<? extends OutputFormat> formatClass) {
        job.setOutputFormatClass(formatClass);
        return this;
    }

    public JobCreator inputFormat(Class<? extends InputFormat> formatClass) {
        job.setInputFormatClass(formatClass);
        return this;
    }

    public Job get() {
        return job;
    }
}
