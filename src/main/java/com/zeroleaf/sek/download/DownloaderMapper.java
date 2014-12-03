package com.zeroleaf.sek.download;

import com.zeroleaf.sek.crawl.FetchEntry;
import com.zeroleaf.sek.data.PageEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.*;

/**
* Created by zeroleaf on 14-12-1.
*/
public class DownloaderMapper
        extends Mapper<LongWritable, FetchEntry, Text, PageEntry> {

    private static Logger LOGGER = LoggerFactory.getLogger(DownloaderMapper.class);

    public static final String THREADS = "sek.download.threads";
    public static final String DURING  = "sek.download.during";
    public static final String LIMIT   = "sek.download.limit";

    private Distributor<FetchEntry> distributor =
            new SimpleDistributor<>(FetchEntry.COMPARATOR);

    private BlockingQueue<HtmlPage> htmlPages = new LinkedBlockingDeque<>();

    private ExecutorService downloaders;

    private int threads;
    private int during;

    private int limit;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        Configuration conf = context.getConfiguration();

        threads = conf.getInt(THREADS, 4);
        during  = conf.getInt(DURING, 60);
        limit   = conf.getInt(LIMIT, -1);

        downloaders = Executors.newFixedThreadPool(threads);
    }

    @Override
    protected void map(LongWritable key, FetchEntry value, Context context)
            throws IOException, InterruptedException {

        LOGGER.debug("添加第 {} 个下载条目 {}", key, value);

        // 每次迭代 key, value 引用的都是同一内存位置的对象.(即开始新一轮迭代时用新数据覆盖旧数据)
        // 因此, 如果直接加到集合中, 那么集合中的元素引用的都将是同一个内存对象! 其值为最后一次迭代时的值.
        // 为了保存以便之后使用, 必须加以复制, 保存其副本.
        distributor.addItem(value.clone());
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {

        super.run(context);

        for (int i = 1; i <= threads; i++) {
            downloaders.execute(new HtmlPageDownloader(
                    String.valueOf(i), limit, distributor, htmlPages));
        }

        HtmlPageWriter htmlPageWriter = new HtmlPageWriter(htmlPages, context);
        Thread t = new Thread(htmlPageWriter);
        t.start();

        downloaders.shutdown();
        downloaders.awaitTermination(during, TimeUnit.MINUTES);

        htmlPageWriter.terminate();
        t.join();
    }

    public static class HtmlPageWriter implements Runnable {

        private static Logger LOGGER = LoggerFactory.getLogger(HtmlPageWriter.class);

        private BlockingQueue<HtmlPage> htmlPages;
        private Context ctx;

        private transient boolean isTerminate;

        public HtmlPageWriter(BlockingQueue<HtmlPage> htmlPages, Context ctx) {
            this.htmlPages = htmlPages;
            this.ctx = ctx;
        }

        @Override
        public void run() {
            while (!isTerminate) {
                writeHtmlPage();
            }

            // 将队列中剩余的网页写入到文件中.
            while (writeHtmlPage())
                ;
        }

        private boolean writeHtmlPage() {
            HtmlPage page = null;
            try {
                page = htmlPages.poll(3, TimeUnit.SECONDS);
                if (page != null) {
                    ctx.write(new Text(page.getUrl()), page.toPageEntry());
                    return true;
                }

                if (!isTerminate) {
                    Thread.sleep(5000);
                }
            } catch (IOException e) {
                if (page.getUrl() != null)
                    LOGGER.error("错误! 无法写入 {} 的数据. 错误信息为 {}",
                            page.getUrl(), e.getCause());
                else
                    LOGGER.error("错误! 无法写入数据. 错误信息为 {}", e.getCause());
            } catch (InterruptedException e) {
                // 忽略
            }
            return false;
        }

        public void terminate() {
            isTerminate = true;
        }
    }

}

