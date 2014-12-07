package com.zeroleaf.sek.download;

import com.zeroleaf.sek.core.AbstractCommand;
import com.zeroleaf.sek.core.DirectoryStructure;

import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zeroleaf on 14-12-1.
 */
public class Downloader extends AbstractCommand {

    private static Logger LOGGER = LoggerFactory.getLogger(Downloader.class);

    private String appDir;

    @Override
    public String getName() {
        return "download";
    }

    @Override
    public void showUsage() {

        HelpFormatter hf = new HelpFormatter();
        hf.printHelp("download [option] <appdir>", downloadArgs);
    }

    @Override
    public void execute(String... args) throws Exception {
        try {
            LOGGER.info("正在执行任务: {}", getName());

            setEnv(args);

            DirectoryStructure dSt = DirectoryStructure.get(appDir);

            runJob(new DownloaderJob(dSt.getFetchlist(false), dSt.getFetchdata()));

            LOGGER.info("任务完成: {}", getName());
        } catch (Exception e) {
            existCode = 10;
            throw e;
        }
    }

    private void setEnv(String ... args) throws Exception {

        try {
            CommandLine cl = new BasicParser().parse(downloadArgs, args);

            if (cl.getArgs().length > 1)
                appDir = cl.getArgs()[1];

            if (cl.hasOption("l"))
                System.setProperty(DownloaderMapper.LIMIT, cl.getOptionValue("l"));
            if (cl.hasOption("t"))
                System.setProperty(DownloaderMapper.DURING, cl.getOptionValue("t"));
            if (cl.hasOption("n"))
                System.setProperty(DownloaderMapper.THREADS, cl.getOptionValue("n"));

            for (String s : cl.getArgs())
                System.out.println(s);

        } catch (ParseException e) {
            LOGGER.error("命令行参数解析错误, 错误消息为 {}", e.getMessage());
        }
    }

    public static void main(String[] args) throws Exception {
        args = new String[]{"download", "-n", "10", "-t", "120", "sek"};
        new Downloader().execute(args);
    }

    // 命令行选项

    private static final Options downloadArgs = new Options();
    static {

        downloadArgs.addOption("l", "size", true, "网页下载的限制大小, 字节为单位")
                    .addOption("t", "during",  true, "指定程序运行时间, 分钟为单位")
                    .addOption("n", "threads", true, "并发下载的线程数量");
    }
}
