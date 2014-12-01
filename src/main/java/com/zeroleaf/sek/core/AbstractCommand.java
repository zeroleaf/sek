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

    public static String NEW_LINE = System.lineSeparator();

    protected int existCode;

    /**
     * @TODO 该方法应该放在 Command 接口中.
     *
     * 显示该命令的用法.
     */
    public void showUsage() {
    }

    protected Path randomDirectory() {
        return FileSystems.randomDirectory(getName() + "-");
    }

    public boolean runJob(Job job)
        throws InterruptedException, IOException, ClassNotFoundException {
        try {
            if (job.waitForCompletion(true)) {
                doAfterSuccess(job);
                return true;
            }
        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            doAfterFail(job);
            throw e;
        }
        return false;
    }

    protected void doAfterSuccess(Job job) throws IOException {
    }

    protected void doAfterFail(Job job) {
    }

    public boolean runJob(SJob sJob)
        throws IOException, ClassNotFoundException, InterruptedException {
        return runJob(sJob.create());
    }

    @Override
    public int getExistCode() {
        return existCode;
    }

    protected static void parseArgs(SekArgs bean, String... args)
        throws CmdLineException {

        CmdLineParser parser = new CmdLineParser(bean);
        parser.parseArgument(args);
    }
}
