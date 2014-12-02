package com.zeroleaf.sek.httpclient;

import com.zeroleaf.sek.download.HtmlPage;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created by zeroleaf on 2014/12/2.
 */
public class HttpClientUtils {

    private static CloseableHttpClient httpClient = HttpClients.createDefault();

    public static CloseableHttpResponse get(String url) throws IOException {
        return httpClient.execute(new HttpGet(url));
    }

//    public static HttpGet toGet(String url) {
//        return new HttpGet(url);
//    }

    public static HtmlPage getFirstNByte(String url, int limit) throws IOException {
        CloseableHttpResponse resp = null;
        try {
            resp = get(url);

            HttpEntity entity = resp.getEntity();
            if (entity == null)
                return null;

            HtmlPage htmlPage = new HtmlPage(url, getContentType(resp));

            int left = limit == -1 ? Integer.MAX_VALUE : limit;
            byte[] buf = new byte[1024];
            InputStream in = entity.getContent();
            while (left > 0) {
                int readCount = in.read(buf);
                if (readCount == -1)
                    break;

                htmlPage.addContent(buf, 0, readCount);
                left -= readCount;
            }
            return htmlPage;
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (resp != null)
                resp.close();
        }
    }

    /**
     * 返回响应的 Content-Type, 如果未设置则返回空字符串.
     *
     * @param resp 响应.
     * @return Content-Type 响应的值.
     */
    private static String getContentType(HttpResponse resp) {
        return getResponseHeader(resp, HttpHeaders.CONTENT_TYPE, "");
    }

    private static String getResponseHeader(HttpResponse resp, String name, String defaultValue) {
        Header header = resp.getFirstHeader(name);
        return header == null ? defaultValue : header.getValue();
    }
}
