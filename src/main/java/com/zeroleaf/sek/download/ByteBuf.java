package com.zeroleaf.sek.download;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

/**
 * Created by zeroleaf on 2014/12/1.
 */
public class ByteBuf {

    private static final byte[] EMPTY_BYTES = new byte[0];

    private byte[] value;

    private int size;

    public ByteBuf() {
        this(1024);
    }

    public ByteBuf(int capacity) {
        this.value = new byte[capacity];
    }

    private void ensureCapacity(int minCapacity) {
        if (minCapacity > value.length)
            expandCapacity(minCapacity);
    }

    void expandCapacity(int minCapacity) {
        int newCapacity = value.length * 2 + 2;
        if (newCapacity - minCapacity < 0)
            newCapacity = minCapacity;

        // @TODO 更精细的控制, 不过对目前的应用来说数组的容量一般不会很大.
        value = Arrays.copyOf(value, newCapacity);
    }

    public void append(byte[] bytes, int offset, int len) {
        ensureCapacity(size + len);

        System.arraycopy(bytes, offset, value, size, len);
        size += len;
    }

    public byte[] get() {
        return Arrays.copyOf(value, size);
    }

    /**
     * 获取某一范围的字节数组.
     *
     * 如果偏移量超过数组长度, 则返回空数组.
     * 如果从偏移量开始没有指定数量的字节, 则返回从偏移量开始到数组结束中所有的字节.
     * 其余情况返回指定偏移量开始的指定字节数构成的字符串.
     *
     * @param offset 第一个字节的偏移量.
     * @param len 获取的字节数组长度.
     * @return 指定范围的字节数组, 可能为空, 或者长度不足.
     */
    public byte[] getRange(int offset, int len) {
        if (offset > size)
            return EMPTY_BYTES;
        final int cLen = Math.min(size - offset, len);
        byte[] dest = new byte[cLen];
        System.arraycopy(value, offset, dest, 0, cLen);
        return dest;
    }

    /**
     * 获取该字节缓冲的大小, 即实际包含的字节数.
     *
     * @return 字节缓冲大小.
     */
    public int size() {
        return size;
    }

    public String toString(String charset) throws UnsupportedEncodingException {
        return new String(value, 0, size, charset);
    }
}
