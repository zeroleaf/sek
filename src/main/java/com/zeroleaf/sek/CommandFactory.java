package com.zeroleaf.sek;

import com.zeroleaf.sek.crawl.Injector;

import java.util.HashSet;
import java.util.Set;

/**
 * @author zeroleaf
 */
public class CommandFactory {

    private static Set<Command> commands = new HashSet<>();

    static {
        commands.add(new Injector());
    }

    /**
     * Find command with the given name.
     *
     * @param name Command name.
     * @return Command for the given name. Or {@code null} if not find.
     */
    public static Command findCommand(String name) {
        for (Command cmd : commands) {
            if (cmd.getName().equals(name)) {
                return cmd;
            }
        }
        return null;
    }
}
