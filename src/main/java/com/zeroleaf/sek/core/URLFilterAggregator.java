package com.zeroleaf.sek.core;

import java.util.HashSet;
import java.util.Set;

/**
 * URLFilter 的辅助类.
 *
 * @author zeroleaf
 */
public class URLFilterAggregator implements URLFilter {

    private Set<URLFilter> filters = new HashSet<>();

    public void addURLFilter(URLFilter filter) {
        this.filters.add(filter);
    }

    public boolean filter(String url) {
        for (URLFilter f : filters) {
            if (f.filter(url)) {
                return true;
            }
        }
        return false;
    }

}
