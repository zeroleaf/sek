package com.zeroleaf.sek.core;


import java.util.HashSet;
import java.util.Set;

/**
 * @author zeroleaf
 */
public class URLNormalizerAggregator implements URLNormalizer {

    private Set<URLNormalizer> normalizers = new HashSet<>();

    public void addURLNormalizer(URLNormalizer normalizer) {
        this.normalizers.add(normalizer);
    }

    public String normalize(String url) {
        String result = url;
        for (URLNormalizer n : normalizers) {
            result = n.normalize(result);
        }
        return result;
    }
}
