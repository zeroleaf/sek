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

    private long fetchTime = System.currentTimeMillis();


    public URLMeta() {
        this(Status.UNFETCHED, 1.0f, 2592000L);
    }

    public URLMeta(Status status, float score, long fetchInterval) {
        this.status = status;
        this.score = score;
        this.fetchInterval = fetchInterval;
    }

    public static URLMeta newInstance() {
        return new URLMeta();
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

    public long getFetchTime() {
        return fetchTime;
    }

    public void setFetchTime(long fetchTime) {
        this.fetchTime = fetchTime;
    }

    public long getFetchInterval() {
        return fetchInterval;
    }

    public void setFetchInterval(long fetchInterval) {
        this.fetchInterval = fetchInterval;
    }
}
