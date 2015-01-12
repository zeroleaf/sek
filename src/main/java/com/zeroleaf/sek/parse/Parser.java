package com.zeroleaf.sek.parse;

import com.zeroleaf.sek.core.AbstractCommand;
import com.zeroleaf.sek.core.DirectoryStructure;
import com.zeroleaf.sek.core.SekException;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zeroleaf on 14-12-3.
 *
 * TODO 解析所生成的数据太大, 元数据只有 174M, 但解析之后的数据却有  13G!
 */
public class Parser extends AbstractCommand {

    private static Logger LOGGER = LoggerFactory.getLogger(Parser.class);

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
            LOGGER.info("正在执行任务: {}", getName());

            if (args.length < 2)
                throw new SekException("<appdir> 必须提供");

            appdir = args[1];

            DirectoryStructure dSt = DirectoryStructure.get(appdir);
            runJob(new ParserJob(dSt.getFetchdata(), dSt.getParsedata()));

            LOGGER.info("任务完成: {}", getName());
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
