package com.zeroleaf.sek.core;

/**
 * @author zeroleaf
 */
public interface Command {

    /**
     * 获取该命令的名称.
     *
     * @return 命令名称.
     */
    String getName();

    /**
     * 执行该命令.
     *
     * @param args 命令的参数和选项.
     * @throws Exception 如果在执行过程中发生异常.
     */
    void execute(String... args) throws Exception;

    /**
     * 获取该命令的退出状态码.
     *
     * 0 表示正常退出. 其余值由具体命令表示各种异常情况.
     * 一般在执行 execute() 方法抛出异常时调用该方法.
     *
     * @return 退出状态码.
     */
    int getExistCode();


}
