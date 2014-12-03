package com.zeroleaf.sek.parse;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by zeroleaf on 14-12-3.
 */
public class Outlink implements Writable {

    private String toUrl;
    private String anchor;

    public Outlink() {
    }

    public Outlink(String toUrl, String anchor) {
        this.toUrl  = toUrl;
        this.anchor = anchor;
    }

    public String getToUrl() {
        return toUrl;
    }

    public String getAnchor() {
        return anchor;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, toUrl);
        Text.writeString(out, anchor);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        toUrl  = Text.readString(in);
        anchor = Text.readString(in);
    }

    public static Outlink read(DataInput in) throws IOException {
        Outlink outlink = new Outlink();
        outlink.readFields(in);
        return outlink;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Outlink outlink = (Outlink) o;

        if (!anchor.equals(outlink.anchor)) return false;
        if (!toUrl.equals(outlink.toUrl)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = toUrl.hashCode();
        result = 31 * result + anchor.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "Outlink{" +
                "toUrl='" + toUrl + '\'' +
                ", anchor='" + anchor + '\'' +
                '}';
    }
}
