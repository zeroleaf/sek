package com.zeroleaf.sek;

import com.zeroleaf.sek.core.Command;
import com.zeroleaf.sek.core.SekException;
import com.zeroleaf.sek.crawl.Generator;
import com.zeroleaf.sek.crawl.Injector;
import com.zeroleaf.sek.download.Downloader;
import com.zeroleaf.sek.merge.Merge;
import com.zeroleaf.sek.parse.Parser;

import java.util.HashSet;
import java.util.Set;

/**
 * @author zeroleaf
 */
public class CommandFactory {

    private static Set<Command> commands = new HashSet<>();

    static {
        commands.add(new Injector());
        commands.add(new Generator());
        commands.add(new Downloader());
        commands.add(new Parser());
        commands.add(new Merge());
        commands.add(new Crawl());
    }

    /**
     * Find command with the given name.
     *
     * @param name Command name.
     * @return Command for the given name. Or {@code null} if not find.
     */
    public static Command find(String name) {
        for (Command cmd : commands) {
            if (cmd.getName().equals(name)) {
                return cmd;
            }
        }
        return null;
    }

    public static void executeCommand(String name, String ... args) throws Exception {
        Command cmd = find(name);
        if (cmd == null)
            throw new SekException("不存在命令 " + name);

        cmd.execute(args);
    }
}
