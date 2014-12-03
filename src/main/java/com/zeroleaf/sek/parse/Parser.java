package com.zeroleaf.sek.parse;

import com.zeroleaf.sek.core.AbstractCommand;
import com.zeroleaf.sek.core.DirectoryStructure;
import com.zeroleaf.sek.core.SekException;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

/**
 * Created by zeroleaf on 14-12-3.
 */
public class Parser extends AbstractCommand {

    private String appdir;

    @Override
    public String getName() {
        return "parse";
    }

    @Override
    public void showUsage() {
        HelpFormatter hf = new HelpFormatter();
        hf.printHelp("parse <appdir>", parseArgs);
    }

    @Override
    public void execute(String... args) throws Exception {
        try {
            if (args.length < 2)
                throw new SekException("<appdir> 必须提供");

            appdir = args[1];

            DirectoryStructure dSt = DirectoryStructure.get(appdir);
            runJob(new ParserJob(dSt.getFetchdata(), dSt.getParsedata()));

        } catch (Exception e) {
            e.printStackTrace();
//            showUsage();
        }
    }

    private static Options parseArgs = new Options();
    static {

    }

    public static void main(String[] args) throws Exception {
        args = new String[]{"parse", "sek"};
        new Parser().execute(args);
    }
}
