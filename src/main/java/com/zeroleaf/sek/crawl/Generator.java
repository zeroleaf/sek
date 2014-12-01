package com.zeroleaf.sek.crawl;

import com.zeroleaf.sek.core.AbstractCommand;
import com.zeroleaf.sek.core.DirectoryStructure;

import java.io.IOException;

/**
 * @TODO 简单实现, 目前仅是按 URL 的评分从高到低排序.
 *
 * 生成此次待抓取列表.
 *
 * @author zeroleaf
 */
public class Generator extends AbstractCommand {

    @Override
    public String getName() {
        return "generate";
    }

    @Override
    public void execute(String... as) throws Exception {
        try {
            GeneratorArgs args = new GeneratorArgs();
            parseArgs(args, as);
            generate(args.getAppDir());
        } catch (Exception e) {
            existCode = 10;
            throw e;
        }
    }

    public void generate(String appDir)
        throws IOException, InterruptedException, ClassNotFoundException {

        DirectoryStructure dSt = DirectoryStructure.get(appDir);

        runJob(new GeneratorJob(dSt.getCrawlDb(), dSt.getFetchlist()));
    }

    public static void main(String[] args) throws Exception {
        new Generator().execute("generate", "sek");
    }
}
