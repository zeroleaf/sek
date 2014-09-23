package com.zeroleaf.sek.core;

import com.zeroleaf.sek.SekConf;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.UUID;

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
}
