package com.zeroleaf.sek.core;

/**
 * @author zeroleaf
 */
public interface URLFilter {

    /**
     * 判断是否过滤给定的 URL.
     *
     * @param url 原始 url.
     * @return 如果要过滤该 URL 返回 true. 否则, false.
     */
    boolean filter(String url);
}
