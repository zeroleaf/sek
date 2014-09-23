package com.zeroleaf.sek.core;

import java.util.HashSet;
import java.util.Set;

/**
 * @author zeroleaf
 */
public class URLNormalizers {

    private static Set<URLNormalizer> normalizers = new HashSet<>();

    static {

    }

    public static String normalize(String url) {
        String result = url;
        for (URLNormalizer n : normalizers) {
            result = n.normalize(url);
        }
        return result;
    }
}
