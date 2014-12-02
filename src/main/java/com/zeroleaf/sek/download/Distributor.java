package com.zeroleaf.sek.download;

/**
 * Created by zeroleaf on 14-12-2.
 *
 * 线程安全的分配器.
 */
public interface Distributor<V> {

    /**
     * 添加新的条目.
     */
    public void addItem(V v);

    /**
     * 获取一个条目.
     *
     * 如果当前没有, 或者 {@code #terminate()} 被调用, 则返回 null.
     * 否则, 返回其所持有的下一个条目.
     *
     * @return 下一个条目 或者 null.
     */
    V getItem();

//    /**
//     * 是否结束.
//     *
//     * 有 2 中结束方式:
//     * 1. 所有的条目都获取完了, 即正常结束.
//     * 2. 调用了 terminate() 方法, 此时条目还未获取完.
//     *
//     * @return 是否结束.
//     */
//    boolean isTerminate();

    /**
     * 结束分配. 调用该方法会使 {@code getItem()} 返回 null.
     *
     */
    void terminate();
}
