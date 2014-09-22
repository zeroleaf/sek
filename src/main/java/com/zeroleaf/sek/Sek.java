package com.zeroleaf.sek;

/**
 * Entry of application.
 *
 * @author zeroleaf
 */
public class Sek {

    private static String findCmdFromArgs(String ... args) {
        for (String arg : args) {
            if (!(arg.startsWith("-") || arg.startsWith("--"))) {
                return arg;
            }
        }
        throw new IllegalArgumentException();
    }

    private static void showUsage() {
        // @TODO 显示程序帮助信息
        System.out.println("Usage");
    }

    public int run(String ... args) {
        try {
            final String cmd = findCmdFromArgs(args);
            System.out.format("Run command %s%n", cmd);
        } catch (IllegalArgumentException e) {
            showUsage();
            System.exit(1);
        }
        return 0;
    }

    public static void main(String[] args) {
        new Sek().run(args);
    }
}
