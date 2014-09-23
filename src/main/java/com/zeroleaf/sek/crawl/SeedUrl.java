package com.zeroleaf.sek.crawl;

/**
* @author zeroleaf
*/
class SeedUrl {

    public static final String SCORE = "score";
    public static final String FETCH_INTERVAL = "fetchInterval";

    private String url;
    private float score = 0.0f;
    private long fetchInterval = 2592000;

    SeedUrl(String input) {
        parse0(input);
    }

    private void parse0(String input) {
        String[] params = input.split("\\s+");
        url = params[0];

        for (int i = 1; i < params.length; i++) {
            if (params[i].contains(SCORE)) {
                score = Float.parseFloat(getValue(params[i]));
            }

            if (params[i].contains(FETCH_INTERVAL)) {
                fetchInterval = Long.parseLong(getValue(params[i]));
            }
        }
    }

    public static SeedUrl parse(String input) {
        return new SeedUrl(input);
    }

    private static String getValue(String pair) {
        return pair.split("=")[1];
    }

    public String getUrl() {
        return url;
    }

    public float getScore() {
        return score;
    }

    public long getFetchInterval() {
        return fetchInterval;
    }

    public URLMeta toURLMeta() {
        return URLMeta.newInstance(score, fetchInterval);
    }

    @Override
    public String toString() {
        return "SeedUrl{" +
               "url='" + url + '\'' +
               ", score=" + score +
               ", fetchInterval=" + fetchInterval +
               '}';
    }
}
