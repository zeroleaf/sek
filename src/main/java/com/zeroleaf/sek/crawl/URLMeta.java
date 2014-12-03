package com.zeroleaf.sek.crawl;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author zeroleaf
 */
public class URLMeta implements Writable, Cloneable {

    /**
     * URL 的状态.
     */
    public static enum Status {
        UNFETCHED,
        FETCHED,
        NOT_EXITS,
        NOT_MODIFIED
    }

    private Status status;

    /**
     * 该 URL 的评分, 是进行排名的一个重要参考数据.
     */
    private float score;

    /**
     * 抓取的时间间隔. 单位为 秒.
     */
    private long fetchInterval;

    /**
     * 该数据的生成时间.
     */
    private long generateTime = System.currentTimeMillis();

    /**
     * 该数据的修改时间.
     */
    private long modifiedTime = -1;

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

    public static URLMeta newDefaultUnfetched() {
        URLMeta meta = new URLMeta();
        meta.status = Status.UNFETCHED;
        return meta;
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

    public long getGenerateTime() {
        return generateTime;
    }

    public long getModifiedTime() {
        return modifiedTime;
    }

    public void setModifiedTime(long modifiedTime) {
        this.modifiedTime = modifiedTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        URLMeta urlMeta = (URLMeta) o;

        if (fetchInterval != urlMeta.fetchInterval) return false;
        if (generateTime != urlMeta.generateTime) return false;
        if (modifiedTime != urlMeta.modifiedTime) return false;
        if (Float.compare(urlMeta.score, score) != 0) return false;
        if (status != urlMeta.status) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = status.hashCode();
        result = 31 * result + (score != +0.0f ? Float.floatToIntBits(score) : 0);
        result = 31 * result + (int) (fetchInterval ^ (fetchInterval >>> 32));
        result = 31 * result + (int) (generateTime ^ (generateTime >>> 32));
        result = 31 * result + (int) (modifiedTime ^ (modifiedTime >>> 32));
        return result;
    }

    @Override
    protected URLMeta clone() {
        try {
            return (URLMeta) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError();
        }
    }

    @Override
    public String toString() {
        return "URLMeta{" +
               "status=" + status +
               ", score=" + score +
               ", fetchInterval=" + fetchInterval +
               ", generateTime=" + generateTime +
               ", modifiedTime=" + modifiedTime +
               '}';
    }
}
