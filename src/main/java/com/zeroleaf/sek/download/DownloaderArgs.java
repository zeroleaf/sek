package com.zeroleaf.sek.download;

import com.zeroleaf.sek.util.SekArgs;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

/**
 * Created by zeroleaf on 14-12-1.
 */
public class DownloaderArgs extends SekArgs {

    @Option(name = "-l", usage = "网页下载的限制大小, 以字节为单位.")
    private Integer limit;

    @Option(name = "-n", usage = "重复下载次数.")
    private Integer repeat;

    @Option(name = "-t", usage = "该程序需要运行多长时间, 分钟为单位.")
    private Integer during;

    @Argument(index = 1, usage = "path of fetchlist")
    private String fetchlist;

    public String getFetchlist() {
        return fetchlist;
    }

    public Integer getLimit() {
        return limit;
    }

    public Integer getRepeat() {
        return repeat;
    }

    public Integer getDuring() {
        return during;
    }

    @Override
    public String toString() {
        return "DownloaderArgs{" +
                "limit=" + limit +
                ", repeat=" + repeat +
                ", during=" + during +
                ", fetchlist='" + fetchlist + '\'' +
                '}';
    }
}
