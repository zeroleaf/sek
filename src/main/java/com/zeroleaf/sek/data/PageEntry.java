package com.zeroleaf.sek.data;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by zeroleaf on 14-12-1.
 */
public class PageEntry implements Writable {

    private int length = 0;
    private Text type = new Text();
    private Text content = new Text();

    // 该网页最近一次的访问时间. 大致的. 如果要精确的时间则应该是 HtmlPage 的生成时间.
    private long modifiedTime = System.currentTimeMillis();

    public PageEntry() {
    }

    public PageEntry(int length, String type, String content) {
        this.length  = length;
        this.type    = new Text(type);
        this.content = new Text(content);
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public Text getType() {
        return type;
    }

    public void setType(Text type) {
        this.type = type;
    }

    public Text getContent() {
        return content;
    }

    public void setContent(Text content) {
        this.content = content;
    }

    public long getModifiedTime() {
        return modifiedTime;
    }

    public void setModifiedTime(long modifiedTime) {
        this.modifiedTime = modifiedTime;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(length);
        type.write(dataOutput);
        content.write(dataOutput);
//        dataOutput.writeLong(modifiedTime);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        length = dataInput.readInt();
        type.readFields(dataInput);
        content.readFields(dataInput);
//        modifiedTime = dataInput.readLong();
    }

    @Override
    public String toString() {
        return "PageEntry{" +
                "length=" + length +
                ", type=" + type +
                ", content=" + content +
                '}';
    }
}
