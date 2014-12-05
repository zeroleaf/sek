package com.zeroleaf.sek.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

/**
 * Created by zeroleaf on 14-12-5.
 *
 * 过滤所有 <em>不是</em> HTTP 协议的 URL.
 */
public class HttpFilter implements URLFilter {

    private static Logger LOGGER = LoggerFactory.getLogger(HttpFilter.class);

    private static Pattern PAT_HTTP = Pattern.compile("^https?://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]");

    @Override
    public boolean filter(String url) {

        boolean match = PAT_HTTP.matcher(url).matches();
        if (!match)
            LOGGER.debug("URL {} 被过滤器 HttpFilter 过滤.", url);
        return !match;
    }
}
