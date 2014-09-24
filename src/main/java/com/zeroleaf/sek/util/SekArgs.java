package com.zeroleaf.sek.util;

import org.kohsuke.args4j.Argument;

/**
 * Sek 程序中所有命令的命令行参数的父类.
 *
 * 命令行参数都要继承该类. 参数位置 0 表示命令的名称.
 * 所以子类的参数位置应从 1 开始.
 *
 * @author zeroleaf
 */
public class SekArgs {

    @Argument(index = 0)
    private String name;

    public String getName() {
        return name;
    }
}
