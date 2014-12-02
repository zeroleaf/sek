package com.zeroleaf.sek.download;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by zeroleaf on 14-12-2.
 */
public class HtmlPageQueue {

    private BlockingQueue<HtmlPage> htmlPages;

    private transient boolean isTerminated;

    public HtmlPageQueue() {
        htmlPages = new LinkedBlockingDeque<>();
    }

    public void add(HtmlPage page) {
        if (!isTerminated)
            htmlPages.add(page);
    }

    public HtmlPage poll() {
        if (!isTerminated)
            return htmlPages.poll();
        return null;
    }

    public void terminate() {
        isTerminated = true;
    }
}
