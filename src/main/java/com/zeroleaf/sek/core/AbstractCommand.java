package com.zeroleaf.sek.core;

import com.zeroleaf.sek.util.SekArgs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import java.io.IOException;
import java.util.UUID;

/**
 * @author zeroleaf
 */
public abstract class AbstractCommand implements Command {

    protected int existCode;

    /**
     * 在当前文件夹下随机生成一个文件夹, 并返回该对象.
     *
     * 生成的文件夹附有前缀, 值为当前命令的名称.
     *
     * @return 随机生成的文件夹.
     */
    protected Path randomPath() {
        return new Path(getName() + "-" + UUID.randomUUID().toString());
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
