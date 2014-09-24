package com.zeroleaf.sek.core;

import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Sek 中的一个 Job.
 *
 * @author zeroleaf
 */
public interface SJob {

    /**
     * 该任务的名称.
     *
     * @return 任务名称.
     */
    String getName();

    /**
     * 创建一个已经配置完整的 Job.
     *
     * @return 配置完整的 Job.
     */
    Job create() throws IOException;
}
