package com.zeroleaf.sek.core;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by zeroleaf on 14-12-5.
 */
public class URLNormalizerAggregatorTest {

    URLNormalizerAggregator normalizerAggregator = new URLNormalizerAggregator();

    @Before
    public void setUp() {
        normalizerAggregator.addURLNormalizer(MarkNormalizer.PARAMETER_NORMALIZER);
        normalizerAggregator.addURLNormalizer(MarkNormalizer.ANCHOR_NORMALIZER);
    }

    @Test
    public void testNormalize() {

        String tUrl1 = "http://123.sogou.com/xyx/cate.php?zt=%E8%BF%9E%E8%BF%9E%E7%9C%8B&fr=2003-0019";
        String t = normalizerAggregator.normalize(tUrl1);
        Assert.assertEquals("http://123.sogou.com/xyx/cate.php", t);
    }
}
