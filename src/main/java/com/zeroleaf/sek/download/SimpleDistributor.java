package com.zeroleaf.sek.download;

import java.util.*;

/**
 * Created by zeroleaf on 14-12-2.
 */
public class SimpleDistributor<V> implements Distributor<V> {

    private boolean isSorted;

    private transient boolean isTerminate = false;

    private LinkedList<V> items = new LinkedList<>();
    private Comparator<V> comparator;


    public SimpleDistributor(Comparator<V> comparator) {
        this.comparator = comparator;
    }

    public synchronized void addItem(V v) {
        items.add(v);
    }

    @Override
    public synchronized V getItem() {
        if (isTerminate || items.isEmpty())
            return null;

        if (!isSorted)
            sortItems();

        return items.removeLast();
    }

    private void sortItems() {
        if (comparator != null)
            Collections.sort(items, comparator);
    }

    @Override
    public void terminate() {
        isTerminate = true;
    }
}
