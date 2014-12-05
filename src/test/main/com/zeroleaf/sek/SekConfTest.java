package com.zeroleaf.sek;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * Created by zeroleaf on 14-12-5.
 */
public class SekConfTest {

    private SekConf sekConf = SekConf.getInstance();

    @Test
    public void testMergeConfig() {
        boolean useParameterNormalizer = sekConf.getBoolean(
                "sek.merge.normalizer.parameter", false);
        assertTrue("默认应是使用 PARAMETER NORMALIZER.", useParameterNormalizer);

        boolean useAnchorNormalizer = sekConf.getBoolean(
                "sek.merge.normalizer.anchor", false);
        assertTrue("默认使用 ANCHOR_NORMALIZER", useAnchorNormalizer);
    }
}
