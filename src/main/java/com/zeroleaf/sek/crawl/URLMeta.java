package com.zeroleaf.sek.crawl;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author zeroleaf
 */
public class URLMeta implements Writable {

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

    public URLMeta() {}

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

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(status.toString());
        out.writeFloat(score);
        out.writeLong(fetchInterval);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        status = Status.valueOf(in.readUTF());
        score = in.readFloat();
        fetchInterval = in.readLong();
    }

    public static URLMeta read(DataInput in) throws IOException {
        URLMeta meta = new URLMeta();
        meta.readFields(in);
        return meta;
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

    @Override
    public String toString() {
        return "URLMeta{" +
               "status=" + status +
               ", score=" + score +
               ", fetchInterval=" + fetchInterval +
               '}';
    }
}
