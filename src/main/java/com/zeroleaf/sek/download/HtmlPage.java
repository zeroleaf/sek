package com.zeroleaf.sek.download;

import com.zeroleaf.sek.data.PageEntry;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
* Created by zeroleaf on 2014/12/2.
*/
public class HtmlPage {

    private String url;

    private String charset;

    private String contentType;

    private ByteBuf byteBuf = new ByteBuf();

    public HtmlPage(String url, String contentType) {
        this.url = url;
        this.contentType = contentType;

        findAndSetCharset(contentType);
    }

    static Pattern CHARSET_PAT = Pattern.compile("charset=([-\\w]+)");

    private boolean findAndSetCharset(String s) {
        Matcher matcher = CHARSET_PAT.matcher(s);
        if (!matcher.find())
            return false;

        charset = matcher.group(1);
        return true;
    }

    public String getUrl() {
        return url;
    }

    public String getContentType() {
        return contentType;
    }

    public int getContentLength() {
        return byteBuf.size();
    }

    public void addContent(byte[] buf, int offset, int len)
            throws IOException {

        byteBuf.append(buf, offset, len);
    }

    public String getContent() {
        if (charset == null) {
            findCharset();
        }
        return bufToCharsetString();
    }

    private String bufToCharsetString() {
        try {
            String csn = charset == null ? "ISO-8859-1" : charset;
            return byteBuf.toString(csn);
        } catch (UnsupportedEncodingException e) {
            return "";
        }
    }

    private static final int TRY_FIRST_BYTES = 4096;

    /**
     * 目前从网页中查找字符编码是仅尝试前 n 个字符.
     * 如果有字符编码信息, 并其出现在 n 个字符之后, 则这种情况无法处理.
     * 一般可通过调节 n 的大小来提高精确度.
     */
    private void findCharset() {
        byte[] bytes = byteBuf.getRange(0, TRY_FIRST_BYTES);
        findAndSetCharset(new String(bytes));
    }

    public PageEntry toPageEntry() {
        return new PageEntry(getContentLength(), getContentType(), getContent());
    }
}
