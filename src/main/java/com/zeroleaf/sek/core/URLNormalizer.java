package com.zeroleaf.sek.core;

/**
 * @author zeroleaf
 */
public interface URLNormalizer {

    /**
     * 对 URL 规范化, 返回规范化之后的 URL.
     *
     * @param url 原始的 URL.
     * @return 规范化之后的 URL.
     */
    String normalize(String url);
}
