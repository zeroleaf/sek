package com.zeroleaf.sek.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zeroleaf on 14-12-5.
 *
 * 标识规范器.
 *
 * 查找指定的标识, 返回标志之前的字符串, 无则返回原始字符串.
 */
public class MarkNormalizer implements URLNormalizer {

    private static Logger LOGGER = LoggerFactory.getLogger(MarkNormalizer.class);

    private final String mark;
    private final String desc;

    public MarkNormalizer(String mark, String desc) {
        this.mark = mark;
        this.desc = desc;
    }

    @Override
    public String normalize(String url) {
        int markIndex = url.indexOf(mark);
        if (markIndex == -1)
            return url;

        String result = url.substring(0, markIndex);
        LOGGER.debug("URL {} 被 {} 规范化为 {}", url, desc, result);
        return result;
    }

    /**
     * URL参数规范化. 返回去除参数之后的URL.
     */
    public static MarkNormalizer PARAMETER_NORMALIZER = new MarkNormalizer("?", "PARAMETER");

    /**
     * URL锚点规范化. 返回去除锚点之后的URL.
     */
    public static MarkNormalizer ANCHOR_NORMALIZER = new MarkNormalizer("#", "ANCHOR");
}
