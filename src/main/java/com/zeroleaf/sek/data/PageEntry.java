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

    private int length;
    private Text type;
    private Text content;

    public PageEntry() {}

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

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(length);
        type.write(dataOutput);
        content.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        length = dataInput.readInt();
        type.readFields(dataInput);
        content.readFields(dataInput);
    }
}
