package org.jetbrains.bio.big

import org.junit.Test
import java.awt.Color
import kotlin.test.assertEquals

/**
 * @author Roman.Chernyatchik
 */
class BedEntryTest {
    @Test fun pack() {
        val e = ExtendedBedEntry(
                "chr1", 10, 30, "be", 5, '+', 15, 25,
                Color(15, 16, 17).rgb, 2, intArrayOf(4, 5), intArrayOf(11, 20),
                arrayOf("val1", "4.55"))

        assertEquals(BedEntry("chr1", 10, 30, "be\t5\t+\t15\t25\t15,16,17\t2\t4,5\t11,20\tval1\t4.55"),
                     e.pack())
    }

    @Test fun packBed3p0() {
        val e = ExtendedBedEntry(
                "chr1", 10, 30, "be", 5, '+', 15, 25,
                Color(15, 16, 17).rgb, 2, intArrayOf(4, 5), intArrayOf(11, 20),
                arrayOf("val1", "4.55"))

        assertEquals(BedEntry("chr1", 10, 30, ""),
                     e.pack(fieldsNumber = 3, extraFieldsNumber = 0))

    }

    @Test fun packBed3pAll() {
        val e = ExtendedBedEntry(
                "chr1", 10, 30, "be", 5, '+', 15, 25,
                Color(15, 16, 17).rgb, 2, intArrayOf(4, 5), intArrayOf(11, 20),
                arrayOf("val1", "4.55"))

        assertEquals(BedEntry("chr1", 10, 30, "val1\t4.55"), e.pack(fieldsNumber = 3))

    }

    @Test fun packBed6pAll() {
        val e = ExtendedBedEntry(
                "chr1", 10, 30, "be", 5, '+', 15, 25,
                Color(15, 16, 17).rgb, 2, intArrayOf(4, 5), intArrayOf(11, 20),
                arrayOf("val1", "4.55"))

        assertEquals(BedEntry("chr1", 10, 30, "be\t5\t+\tval1\t4.55"),
                     e.pack(fieldsNumber = 6))
    }

    @Test fun packBed6p0() {
        val e = ExtendedBedEntry(
                "chr1", 10, 30, "be", 5, '+', 15, 25,
                Color(15, 16, 17).rgb, 2, intArrayOf(4, 5), intArrayOf(11, 20),
                arrayOf("val1", "4.55"))

        assertEquals(BedEntry("chr1", 10, 30, "be\t5\t+"),
                     e.pack(fieldsNumber = 6, extraFieldsNumber = 0))
    }

    @Test fun packBed6p1() {
        val e = ExtendedBedEntry(
                "chr1", 10, 30, "be", 5, '+', 15, 25,
                Color(15, 16, 17).rgb, 2, intArrayOf(4, 5), intArrayOf(11, 20),
                arrayOf("val1", "4.55"))

        assertEquals(BedEntry("chr1", 10, 30, "be\t5\t+\tval1"),
                     e.pack(fieldsNumber = 6, extraFieldsNumber = 1))
    }

    @Test fun packBed6p2() {
        val e = ExtendedBedEntry(
                "chr1", 10, 30, "be", 5, '+', 15, 25,
                Color(15, 16, 17).rgb, 2, intArrayOf(4, 5), intArrayOf(11, 20),
                arrayOf("val1", "4.55"))

        assertEquals(BedEntry("chr1", 10, 30, "be\t5\t+\tval1\t4.55"),
                     e.pack(fieldsNumber = 6, extraFieldsNumber = 2))
    }

    @Test fun packBed9p2() {
        val e = ExtendedBedEntry(
                "chr1", 10, 30, "be", 5, '+', 15, 25,
                Color(15, 16, 17).rgb, 2, intArrayOf(4, 5), intArrayOf(11, 20),
                arrayOf("val1", "4.55"))

        assertEquals(BedEntry("chr1", 10, 30, "be\t5\t+\t15\t25\t15,16,17\tval1\t4.55"),
                     e.pack(fieldsNumber = 9, extraFieldsNumber = 2))
    }

    @Test fun packBed12p0() {
        val e = ExtendedBedEntry(
                "chr1", 10, 30, "be", 5, '+', 15, 25,
                Color(15, 16, 17).rgb, 2, intArrayOf(4, 5), intArrayOf(11, 20),
                arrayOf("val1", "4.55"))

        assertEquals(BedEntry("chr1", 10, 30, "be\t5\t+\t15\t25\t15,16,17\t2\t4,5\t11,20"),
                     e.pack(fieldsNumber = 12, extraFieldsNumber = 0))
    }

    @Test fun packBed12p2() {
        val e = ExtendedBedEntry(
                "chr1", 10, 30, "be", 5, '+', 15, 25,
                Color(15, 16, 17).rgb, 2, intArrayOf(4, 5), intArrayOf(11, 20),
                arrayOf("val1", "4.55"))

        assertEquals(BedEntry("chr1", 10, 30, "be\t5\t+\t15\t25\t15,16,17\t2\t4,5\t11,20\tval1\t4.55"),
                     e.pack(fieldsNumber = 12, extraFieldsNumber = 2))
    }

    @Test(expected = IllegalStateException::class)
    fun packBed2() {
        val e = ExtendedBedEntry(
                "chr1", 10, 30, "be", 5, '+', 15, 25,
                Color(15, 16, 17).rgb, 2, intArrayOf(4, 5), intArrayOf(11, 20),
                arrayOf("val1", "4.55"))
        e.pack(fieldsNumber = 2)
    }

    @Test(expected = IllegalStateException::class)
    fun packBed13() {
        val e = ExtendedBedEntry(
                "chr1", 10, 30, "be", 5, '+', 15, 25,
                Color(15, 16, 17).rgb, 2, intArrayOf(4, 5), intArrayOf(11, 20),
                arrayOf("val1", "4.55"))
        e.pack(fieldsNumber = 13)
    }

    @Test(expected = IllegalStateException::class)
    fun packWrongSizes() {
        ExtendedBedEntry("chr1", 10, 30, blockCount = 2,
                         blockSizes = intArrayOf(4), blockStarts = intArrayOf(11, 20)
        ).pack()
    }

    @Test(expected = IllegalStateException::class)
    fun packWrongStarts() {
        ExtendedBedEntry("chr1", 10, 30, blockCount = 2,
                                 blockSizes = intArrayOf(4, 5), blockStarts = intArrayOf(11)
        ).pack()
    }

    @Test fun packNoColor() {
        val e = ExtendedBedEntry("chr1", 10, 30, itemRgb = 0)

        assertEquals(BedEntry("chr1", 10, 30, "\t0\t.\t0\t0\t0"),
                     e.pack(fieldsNumber = 9, extraFieldsNumber = 0))

    }

    @Test fun packNoBlocks() {
        val e = ExtendedBedEntry("chr1", 10, 30)

        assertEquals(BedEntry("chr1", 10, 30, "\t0\t.\t0\t0\t0\t0\t.\t."),
                     e.pack(fieldsNumber = 12, extraFieldsNumber = 0))

    }

    @Test
    fun unpack() {
        val bedEntries = listOf(
                ExtendedBedEntry("chr1", 1, 100, ".", 0, '+',
                                 15, 25,
                                 Color(15, 16, 17).rgb, 2, intArrayOf(4, 5), intArrayOf(11, 20),
                                 extraFields = arrayOf("34.56398", "-1.00000", "4.91755", "240")),
                ExtendedBedEntry("chr1", 200, 300, ".", 800, '.',
                                 20, 22,
                                 0, 0,
                                 extraFields = arrayOf("193.07668", "-1.00000", "4.91755", "171"))
        )
        assertEquals(bedEntries, bedEntries.map { it.pack().unpack(extraFieldsNumber = 4) })
    }

    @Test fun unpackBed3() {
        val bedEntry = BedEntry("chr1", 1, 100, "")
        val expected = ExtendedBedEntry("chr1", 1, 100)
        val actual = bedEntry.unpack(fieldsNumber = 3)
        assertEquals(expected,
                     actual
        )
    }

    @Test fun unpackBed3as12() {
        val bedEntry = BedEntry("chr1", 1, 100, "")
        assertEquals(ExtendedBedEntry("chr1", 1, 100),
                     bedEntry.unpack(fieldsNumber = 12)
        )
    }

    @Test fun unpackBed3p0() {
        val bedEntry = BedEntry("chr1", 1, 100, ".\t4\t+\t34.56398\t-1.00000\t4.91755\t240")
        assertEquals(ExtendedBedEntry("chr1", 1, 100),
                     bedEntry.unpack(fieldsNumber = 3, extraFieldsNumber = 0)
        )
    }

    @Test fun unpackBed3p4Partial() {
        val bedEntry = BedEntry("chr1", 1, 100, ".\t4\t+\t34.56398\t-1.00000\t4.91755\t240")
        assertEquals(ExtendedBedEntry("chr1", 1, 100,
                                      extraFields = arrayOf(".", "4", "+", "34.56398")),
                     bedEntry.unpack(fieldsNumber = 3, extraFieldsNumber = 4)
        )
    }

    @Test fun unpackBed3p4() {
        val bedEntry = BedEntry("chr1", 1, 100, "34.56398\t-1.00000\t4.91755\t240")
        assertEquals(ExtendedBedEntry("chr1", 1, 100,
                                      extraFields = arrayOf("34.56398", "-1.00000", "4.91755", "240")),
                     bedEntry.unpack(fieldsNumber = 3, extraFieldsNumber = 4)
        )
    }

    @Test fun unpackBed6p4() {
        val bedEntry = BedEntry("chr1", 1, 100, ".\t4\t+\t34.56398\t-1.00000\t4.91755\t240")
        assertEquals(ExtendedBedEntry("chr1", 1, 100, ".", 4, '+',
                                      extraFields = arrayOf("34.56398", "-1.00000", "4.91755", "240")),
                     bedEntry.unpack(fieldsNumber = 6, extraFieldsNumber = 4)
        )
    }

    @Test fun unpackBedEmptyName() {
        val bedEntry = BedEntry("chr1", 1, 100, "\t4\t+")
        assertEquals(ExtendedBedEntry("chr1", 1, 100, "", 4, '+'),
                     bedEntry.unpack(fieldsNumber = 6)
        )
    }

    @Test fun unpackBedEmptyExtraFields1() {
        val bedEntry = BedEntry("chr1", 1, 100, "")
        assertEquals(ExtendedBedEntry("chr1", 1, 100),
                     bedEntry.unpack(fieldsNumber = 3)
        )
    }

    @Test fun unpackBedEmptyExtraFields2() {
        val bedEntry = BedEntry("chr1", 1, 100, "foo\t")
        assertEquals(ExtendedBedEntry("chr1", 1, 100, "foo"),
                     bedEntry.unpack(fieldsNumber = 4)
        )
    }

    @Test fun unpackMoreExtraFieldsThanNeeded() {
        val bedEntry = BedEntry("chr1", 1, 100, ".\t4\t+\t34.56398\t-1.00000")
        assertEquals(ExtendedBedEntry("chr1", 1, 100, ".", 4, '+',
                                      extraFields = arrayOf("34.56398", "-1.00000")),
                     bedEntry.unpack(fieldsNumber = 6, extraFieldsNumber = 4)
        )
    }

    @Test fun unpackLessExtraFieldsThanNeeded() {
        val bedEntry = BedEntry("chr1", 1, 100, ".\t4\t+")
        assertEquals(ExtendedBedEntry("chr1", 1, 100, ".",
                                      extraFields = arrayOf("4", "+")),
                     bedEntry.unpack(fieldsNumber = 4, extraFieldsNumber = 4)
        )
    }
    @Test fun unpackNoExtraFieldsWhenNeeded() {
        val bedEntry = BedEntry("chr1", 1, 100, "\t4\t+")
        assertEquals(ExtendedBedEntry("chr1", 1, 100, "", 4, '+'),
                     bedEntry.unpack(fieldsNumber = 6, extraFieldsNumber = 1)
        )
    }

    @Test fun unpackBed4pAll() {
        val bedEntry = BedEntry("chr1", 1, 100, "foo\t34.56398\t-1.00000\t4.91755\t240")
        assertEquals(ExtendedBedEntry("chr1", 1, 100, "foo",
                                      extraFields = arrayOf("34.56398", "-1.00000", "4.91755", "240")),
                     bedEntry.unpack(fieldsNumber = 4)
        )
    }

    @Test(expected = IllegalStateException::class)
    fun unpackBed2() {
        BedEntry("chr1", 1, 100, "").unpack(fieldsNumber = 2)
    }

    @Test(expected = IllegalStateException::class)
    fun unpackBed13() {
        BedEntry("chr1", 1, 100, "").unpack(fieldsNumber = 13)
    }
}