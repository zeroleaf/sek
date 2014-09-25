package com.zeroleaf.sek.crawl;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author zeroleaf
 */
public class FetchEntry implements WritableComparable<FetchEntry> {

    private Text url;
    private URLMeta meta;

    public FetchEntry() {
        url = new Text();
        meta = new URLMeta();
    }

    public FetchEntry(Text url, URLMeta meta) {
        this.url = url;
        this.meta = meta;
    }

    public Text getUrl() {
        return url;
    }

    public void setUrl(Text url) {
        this.url = url;
    }

    @Override
    public int compareTo(FetchEntry o) {
        // @TODO 简单实现, 目前仅根据分数比较.
        final float ft = meta.getScore();
        final float fo = o.meta.getScore();
        return ft > fo ? 1 : ft == fo ? 0 : -1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        url.write(out);
        meta.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        url.readFields(in);
        meta.readFields(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FetchEntry)) {
            return false;
        }

        FetchEntry that = (FetchEntry) o;

        if (!meta.equals(that.meta)) {
            return false;
        }
        return url.equals(that.url);
    }

    @Override
    public int hashCode() {
        int result = url.hashCode();
        result = 31 * result + meta.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "FetchEntry{" +
               "url=" + url +
               ", meta=" + meta +
               '}';
    }
}
