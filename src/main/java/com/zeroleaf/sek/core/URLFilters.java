package com.zeroleaf.sek.core;

import java.util.HashSet;
import java.util.Set;

/**
 * URLFilter 的辅助类.
 *
 * @author zeroleaf
 */
public class URLFilters {

    private static Set<URLFilter> filters = new HashSet<>();

    static {

    }

    public static boolean filter(String url) {
        for (URLFilter f : filters) {
            if (f.filter(url)) {
                return true;
            }
        }
        return false;
    }

}
