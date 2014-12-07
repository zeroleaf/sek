package com.zeroleaf.sek.parse;

import com.zeroleaf.sek.data.PageEntry;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by zeroleaf on 14-12-3.
 */
public class ParsedEntry implements Writable, Cloneable {

    public static Comparator<ParsedEntry> MODIFIED_TIME_COMPARATOR =
            new Comparator<ParsedEntry>() {
        @Override
        public int compare(ParsedEntry o1, ParsedEntry o2) {
            return o1.modifiedTime > o2.modifiedTime ? 1 :
                    o1.modifiedTime == o2.modifiedTime ? 0 : -1;
        }
    };

    private String content;

    private HashSet<Outlink> outlinks = new HashSet<>();

    private String type;

    private int length;

    private boolean truncated;

    private long modifiedTime = -1;

    public ParsedEntry() {}

    public static ParsedEntry valueOf(PageEntry page) {
        ParsedEntry result = new ParsedEntry();
        result.content = page.getContent().toString();
        result.length = page.getLength();
        result.type = page.getType().toString();
//        result.truncated = page
        result.modifiedTime = page.getModifiedTime();
        return result;
    }

    public String getContent() {
        return content;
    }

    public Set<Outlink> getOutlinks() {
        return outlinks;
    }

    public String getType() {
        return type;
    }

    public int getLength() {
        return length;
    }

    public boolean isTruncated() {
        return truncated;
    }

    public long getModifiedTime() {
        return modifiedTime;
    }

    public void addOutlink(Outlink outlink) {
        outlinks.add(outlink);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, content);
        out.writeInt(outlinks.size());
        for (Outlink outlink : outlinks)
            outlink.write(out);
        Text.writeString(out, type);
        out.writeInt(length);
        out.writeBoolean(truncated);
        out.writeLong(modifiedTime);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        content = Text.readString(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++)
            outlinks.add(Outlink.read(in));
        type = Text.readString(in);
        length = in.readInt();
        truncated = in.readBoolean();
        modifiedTime = in.readLong();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ParsedEntry that = (ParsedEntry) o;

        if (length != that.length) return false;
        if (modifiedTime != that.modifiedTime) return false;
        if (truncated != that.truncated) return false;
        if (!content.equals(that.content)) return false;
        if (!type.equals(that.type)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = content.hashCode();
        result = 31 * result + type.hashCode();
        result = 31 * result + length;
        result = 31 * result + (truncated ? 1 : 0);
        result = 31 * result + (int) (modifiedTime ^ (modifiedTime >>> 32));
        return result;
    }

    @Override
    public ParsedEntry clone() {
        ParsedEntry clone = null;
        try {
            clone = (ParsedEntry) super.clone();
            clone.outlinks = (HashSet<Outlink>) outlinks.clone();
            return clone;
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public String toString() {
        return "ParsedEntry{" +
                "content='" + content + '\'' +
                ", outlinks=" + outlinks +
                ", type='" + type + '\'' +
                ", length=" + length +
                ", truncated=" + truncated +
                ", modifiedTime=" + modifiedTime +
                '}';
    }
}
