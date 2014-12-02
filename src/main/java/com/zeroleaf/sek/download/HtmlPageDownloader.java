package com.zeroleaf.sek.download;

import com.zeroleaf.sek.crawl.FetchEntry;
import com.zeroleaf.sek.httpclient.HttpClientUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

/**
 * Created by zeroleaf on 2014/12/2.
 */
public class HtmlPageDownloader implements Runnable {

    private static Logger LOGGER = LoggerFactory.getLogger(HtmlPageDownloader.class);

    private Distributor<FetchEntry> distributor;
    private BlockingQueue<HtmlPage> htmlPages;


    private String identifier;
    private int limit;


    public HtmlPageDownloader(String identifier, int limit,
                              Distributor<FetchEntry> distributor,
                              BlockingQueue<HtmlPage> htmlPages) {

        this.identifier  = identifier;
        this.limit       = limit;
        this.distributor = distributor;
        this.htmlPages   = htmlPages;
    }

    @Override
    public void run() {
        FetchEntry entry;
        while ((entry = distributor.getItem()) != null) {
            try {
                LOGGER.info("[线程 {}]: 开始下载 {} ...", identifier, entry.getUrl());
                HtmlPage page = HttpClientUtils.getFirstNByte(entry.getUrl().toString(), limit);
                LOGGER.info("[线程 {}]: 下载完成 {}.", identifier, entry.getUrl());
                htmlPages.add(page);

                Thread.sleep(3000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        LOGGER.info("[线程 {}]: 没有网页需要下载, 任务完成.", identifier);
    }
}
