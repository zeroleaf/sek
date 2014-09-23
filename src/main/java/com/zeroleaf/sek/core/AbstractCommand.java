package com.zeroleaf.sek.core;

import com.zeroleaf.sek.SekConf;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @author zeroleaf
 */
public abstract class AbstractCommand implements Command {

    @Override
    public void execute(String... args) throws Exception {
        ensureArgs(args);
    }

    protected void ensureArgs(String ... args) {
    }

    protected static Job createJob() throws IOException {
        Job job = Job.getInstance(new SekConf());
        job.setJarByClass(AbstractCommand.class);
        return job;
    }
}
