package com.zeroleaf.sek.core;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by zeroleaf on 14-12-2.
 */
public class DirectoryStructureTest {

    private DirectoryStructure dSt;

    @Before
    public void init() {
        dSt = DirectoryStructure.get("sek");
    }

    @Test
    public void testFindLastSegment() {

        Assert.assertEquals("20141201134459", dSt.findLastSegment().toString());
    }
}
