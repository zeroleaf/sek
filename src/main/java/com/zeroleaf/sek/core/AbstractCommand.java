package com.zeroleaf.sek.core;

import com.zeroleaf.sek.util.FileSystems;
import com.zeroleaf.sek.util.SekArgs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.io.IOException;

/**
 * @author zeroleaf
 */
public abstract class AbstractCommand implements Command {

    protected int existCode;

    protected Path randomDirectory() {
        return FileSystems.randomDirectory(getName() + "-");
    }

    public boolean runJob(Job job) {
        try {
            if (job.waitForCompletion(true)) {
                doAfterSuccess(job);
                return true;
            }
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        doAfterFail(job);
        return false;
    }

    protected void doAfterSuccess(Job job) throws IOException {
    }

    protected void doAfterFail(Job job) {
    }

    public boolean runJob(SJob sJob) throws IOException {
        return runJob(sJob.create());
    }

    @Override
    public int getExistCode() {
        return existCode;
    }

    protected static void parseArgs(SekArgs bean, String ... args)
        throws CmdLineException {
        CmdLineParser parser = new CmdLineParser(bean);
        parser.parseArgument(args);
    }
}
