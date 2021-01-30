package com.gooddata.jdbc.util;

import org.testng.annotations.Test;

import java.util.List;

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
        assert(!TextUtil.isGoodDataColumnWithUri("Revenue"));
    }


    @Test
    public void testFindUris() throws TextUtil.InvalidFormatException {
        List<String> uris = TextUtil.findAllObjectUris("SELECT " +
                "[/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/465] " +
                "WHERE [/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/271] " +
                "IN ([/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/271/elements?id=48])");
        assert (uris.contains("/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/271"));
        assert (uris.contains("/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/465"));
        assert (!uris.contains("/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/271/elements?id=48"));

        uris = TextUtil.findAllElementUris("SELECT " +
                "[/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/465] " +
                "WHERE [/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/271] " +
                "IN ([/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/271/elements?id=48])");
        assert (!uris.contains("/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/271"));
        assert (!uris.contains("/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/465"));
        assert (uris.contains("/gdc/md/w2x7a9awsioch4l9lbzgjcn99hbkm61e/obj/271/elements?id=48"));
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
