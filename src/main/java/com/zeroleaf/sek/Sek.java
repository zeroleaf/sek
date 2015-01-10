package com.zeroleaf.sek;

import com.zeroleaf.sek.core.Command;
import com.zeroleaf.sek.core.SekException;

import static com.zeroleaf.sek.CommandFactory.find;

/**
 * Entry of application.
 *
 * @author zeroleaf
 */
public class Sek {

    private static String findCmdNameFromArgs(String ... args) {
        for (String arg : args) {
            if (!(arg.startsWith("-") || arg.startsWith("--"))) {
                return arg;
            }
        }
        return null;
    }

    private static Command findCmdFromArgs(String ... args) {
        final String name = findCmdNameFromArgs(args);
        if (name == null) {
            throw new IllegalArgumentException();
        }
        final Command cmd = find(name);
        if (cmd == null) {
            throw new IllegalArgumentException("Unknown command " + name);
        }
        return cmd;
    }

    /**
     * Show usage with specified error message.
     *
     * @param msg Error message, may be null.
     */
    private static void showUsage(String msg) {
        // @TODO 显示程序帮助信息
        System.out.format("发生错误, 信息为: %s", msg);
    }

    public int run(String ... args) {
        Command cmd = null;
        try {
            cmd = findCmdFromArgs(args);
            cmd.execute(args);
        } catch (SekException e) {
            showUsage(e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            System.err.println("发生未知错误! 信息为: " + e.getMessage());
            assert cmd != null;
            System.exit(cmd.getExistCode());
        }
        return 0;
    }

    public static void main(String[] args) {
        args = new String[]{"crawl", "-t", "3", "sek", "urls"};
        new Sek().run(args);
    }
}
