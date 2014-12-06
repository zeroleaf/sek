package com.zeroleaf.sek.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zeroleaf on 14-12-6.
 */
public class CollectionUtils {



    public static <T> List<T> iterToList(Iterable<T> iter) {
        List<T> list = new ArrayList<>();
        for (T t : iter)
            list.add(t);
        return list;
    }

    private CollectionUtils() {}
}
