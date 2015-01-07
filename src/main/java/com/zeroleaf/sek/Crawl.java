package com.zeroleaf.sek;

import com.zeroleaf.sek.core.AbstractCommand;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zeroleaf on 14-12-7.
 */
public class Crawl extends AbstractCommand {

    private static Logger LOGGER = LoggerFactory.getLogger(Crawl.class);

    private static CommandLine cl;

    private static int repeat = 1;
    private static int timeRemain = -1;

    @Override
    public String getName() {
        return "crawl";
    }

    @Override
    public void execute(String... args) throws Exception {
        try {
            setEnv(args);

            while (repeat-- > 0) {

                long start = System.currentTimeMillis();

                CommandFactory.executeCommand("inject",   getAppdir(), getSeedurl());
                CommandFactory.executeCommand("generate", getAppdir());
                CommandFactory.executeCommand("download", getDownloadArgs());
                CommandFactory.executeCommand("parse",    getAppdir());
                CommandFactory.executeCommand("merge",    getAppdir());

                int timeEscapeMin = (int) ((System.currentTimeMillis() - start) / 1000 / 60);
                timeRemain -= timeEscapeMin;

                if (timeRemain < 0)
                    break;

                int rtoIdx = getRunTimeOptionIndex(args);
                if (rtoIdx != -1)
                    args[rtoIdx + 1] = String.valueOf(timeRemain);

            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    private static String[] getDownloadArgs() {
        StringBuilder sb = new StringBuilder("Download ");
        if (timeRemain > 0)
            sb.append(" -t ").append(timeRemain);
        if (cl.hasOption('l'))
            sb.append(" -l ").append(cl.getOptionValue("l"));
        if (cl.hasOption('n'))
            sb.append(" -n ").append(cl.getOptionValue("n"));

        sb.append(" ").append(getAppdir());
        return sb.toString().split("\\s+");
    }

    private static String getAppdir() {
        return cl.getArgs()[1];
    }

    private static String getSeedurl() {
        return cl.getArgs()[2];
    }

    private int getRunTimeOptionIndex(String ... args) {
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-r") || args[i].equals("--during"))
                return i;
        }
        return -1;
    }

    private void setEnv(String ... args) throws ParseException {
        cl = new BasicParser().parse(crawlArgs, args);

        if (cl.hasOption("r")) {
            repeat = Integer.valueOf(cl.getOptionValue("r"));
        }
        if (cl.hasOption("t")) {
            timeRemain = Integer.valueOf(cl.getOptionValue("t"));
        }
    }

    public static void main(String[] args) throws Exception {
        args = new String[]{"crawl", "sek", "urls"};
        new Crawl().execute(args);
    }

    private static Options crawlArgs = new Options();
    static {
        crawlArgs.addOption("r", "repeat",  true, "下载需要迭代的次数. 注: 实际迭代次数由下载时间决定.");
        crawlArgs.addOption("t", "during",  true, "指定程序运行时间, 分钟为单位");
        crawlArgs.addOption("l", "size",    true, "网页下载的限制大小, 字节为单位");
        crawlArgs.addOption("n", "threads", true, "并发下载的线程数量");
    }
}
