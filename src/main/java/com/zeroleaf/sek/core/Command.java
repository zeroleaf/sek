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
     *
     * 特别的, 第一个参数为该命令的名称, 往后才是该命令在运行时所需要的具体参数.
     * 必须对参数进行校验, 验证其合法性;同时, 对于有特殊要求的参数, 如文件夹等,
     * 要对其进行验证, 在验证不通过时及时地抛出异常说明发生的具体错误.
     *
     * 命令行参数的优先级是最高的, 因此对命令行中的选项,必须设置相应的系统属性,
     * 以保证 MapReduce 任务以命令行参数优先.
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
