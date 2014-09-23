package com.zeroleaf.sek.crawl;

/**
 * @author zeroleaf
 */
public class URLMeta {

    public static enum Status {
        UNFETCHED,
        FETCHED,
        NOT_EXITS,
        NOT_MODIFIED
    }

    private Status status;

    private float score;

    /**
     * 抓取的时间间隔. 单位为 秒.
     */
    private long fetchInterval;

    public URLMeta(float score, long fetchInterval) {
        this(Status.UNFETCHED, score, fetchInterval);
    }

    public URLMeta(Status status, float score, long fetchInterval) {
        this.status = status;
        this.score = score;
        this.fetchInterval = fetchInterval;
    }

    public static URLMeta newInstance(float score, long fetchInterval) {
        return new URLMeta(score, fetchInterval);
    }


    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public float getScore() {
        return score;
    }

    public void setScore(float score) {
        this.score = score;
    }

    public long getFetchInterval() {
        return fetchInterval;
    }

    public void setFetchInterval(long fetchInterval) {
        this.fetchInterval = fetchInterval;
    }
}
