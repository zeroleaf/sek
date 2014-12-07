package com.zeroleaf.sek.crawl;

import com.zeroleaf.sek.core.AbstractCommand;
import com.zeroleaf.sek.core.DirectoryStructure;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @TODO 简单实现, 目前仅是按 URL 的评分从高到低排序.
 *
 * 生成此次待抓取列表.
 *
 * @author zeroleaf
 */
public class Generator extends AbstractCommand {

    private static Logger LOGGER = LoggerFactory.getLogger(Generator.class);

    @Override
    public String getName() {
        return "generate";
    }

    @Override
    public void execute(String... as) throws Exception {
        try {
            LOGGER.info("正在执行任务: {}", getName());
//            GeneratorArgs args = new GeneratorArgs();
//            parseArgs(args, as);

            if (as.length < 2)
                throw new IllegalArgumentException("<appdir> 必须");

            String appdir = as[1];
            generate(appdir);
            LOGGER.info("任务完成: {}", getName());
        } catch (Exception e) {
            existCode = 10;
            throw e;
        }
    }

    public void generate(String appDir)
        throws IOException, InterruptedException, ClassNotFoundException {

        DirectoryStructure dSt = DirectoryStructure.get(appDir);

        runJob(new GeneratorJob(dSt.getCrawlDb(), dSt.getFetchlist(true)));
    }

    private static Options generateArgs = new Options();
    static {
    }

    public static void main(String[] args) throws Exception {
        new Generator().execute("generate", "sek");
    }
}
