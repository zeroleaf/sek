package com.zeroleaf.sek.download;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by zeroleaf on 14-12-2.
 */
public class SimpleDistributor<V> implements Distributor<V> {

    private static Logger LOGGER = LoggerFactory.getLogger(SimpleDistributor.class);

    private boolean isSorted;

    private transient boolean isTerminate = false;

    private LinkedList<V> items = new LinkedList<>();
    private Comparator<V> comparator;


    public SimpleDistributor(Comparator<V> comparator) {
        this.comparator = comparator;
    }

    public synchronized void addItem(V v) {
        LOGGER.debug("添加新条目 {}", v);
        items.add(v);
        isSorted = false;
    }

    @Override
    public synchronized V getItem() {
        if (isTerminate || items.isEmpty())
            return null;

        if (!isSorted)
            sortItems();

        V item = items.removeLast();
        LOGGER.debug("提供条目 {}", item);
        return item;
    }

    private void sortItems() {
        if (comparator != null)
            Collections.sort(items, comparator);
        isSorted = true;
    }

    @Override
    public void terminate() {
        isTerminate = true;
    }
}
