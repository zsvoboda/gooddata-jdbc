package com.gooddata.jdbc.util;

import org.testng.annotations.Test;

public class TestTextUtil {

    @Test
    public void testExtractPid() throws TextUtil.InvalidFormatException {
        assert(
                TextUtil.extractIdFromUri("/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/386").
                        equals("w2x7a9awsioch4l9lbzgjcn99hbkm61e"));
        assert(
                !TextUtil.extractIdFromUri("/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/386").
                        equals("obj"));
    }

    @Test
    public void testUri() throws TextUtil.InvalidFormatException {
        assert(TextUtil.isGoodDataObjectUri("/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/386"));
        assert(TextUtil.isGoodDataColumnWithUri("[/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/386]"));
        assert(TextUtil.isGoodDataColumnWithUri(" [ /gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/386 ] "));
        assert(TextUtil.extractGoodDataUriFromColumnName(" [ /gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/386 ] ")
                .equals("/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/386"));
        assert(TextUtil.extractGoodDataUriFromColumnName("[ /gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/386 ] ")
                .equals("/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/386"));
        assert(TextUtil.extractGoodDataUriFromColumnName(" [ /gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/386 ]")
                .equals("/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/386"));
        assert(TextUtil.extractGoodDataUriFromColumnName("[/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/386 ]")
                .equals("/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/386"));
        assert(TextUtil.extractGoodDataUriFromColumnName("[/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/386]")
                .equals("/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/386"));
    }

    @Test(expectedExceptions = { TextUtil.InvalidFormatException.class })
    public void testParseBoolException() throws TextUtil.InvalidFormatException {
        TextUtil.extractIdFromUri("/gdc/obj/w2x7a9awsioch4l9lbzgjcn99hbkm61e/s386");
    }

    @Test(expectedExceptions = { TextUtil.InvalidFormatException.class })
    public void testUriFormat() throws TextUtil.InvalidFormatException {
        TextUtil.extractIdFromUri("/gdc/abj/w2x7a9awsioch4l9lbzgjcn99hbkm61e/s386");
        TextUtil.extractGoodDataUriFromColumnName("[/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/abj/386]");
        TextUtil.extractGoodDataUriFromColumnName("[/gdc/projects/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/386]");
        TextUtil.extractGoodDataUriFromColumnName("/gdc/projects/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/386");
    }

}
