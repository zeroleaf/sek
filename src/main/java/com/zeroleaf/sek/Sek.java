package com.zeroleaf.sek;

import com.zeroleaf.sek.core.Command;
import com.zeroleaf.sek.core.CommandException;

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
        System.out.format("Error message : %s, and this is usage", msg);
    }

    public int run(String ... args) {
        try {
            final Command cmd = findCmdFromArgs(args);
            cmd.execute(args);
        } catch (IllegalArgumentException e) {
            showUsage(e.getMessage());
            System.exit(1);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(2);
        }
        return 0;
    }

    public static void main(String[] args) {
        new Sek().run(args);
    }
}
