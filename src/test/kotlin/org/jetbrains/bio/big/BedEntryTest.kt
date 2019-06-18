package org.jetbrains.bio.big

import org.junit.Assert
import org.junit.Test
import java.awt.Color
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * @author Roman.Chernyatchik
 */
class BedEntryTest {

    @Test fun pack() {
        val expected = BedEntry(
            "chr1", 10, 30, "be\t5\t+\t15\t25\t15,16,17\t2\t4,5\t11,20\tval1\t4.55"
        )
        assertEquals(expected, BED_ENTRY_12_P_2.pack())
        assertEquals(expected.rest.split("\t"), BED_ENTRY_12_P_2.rest())
    }

    @Test fun packBed3p0() {
        val expected = BedEntry("chr1", 10, 30, "")
        assertPackAndRest(expected, BED_ENTRY_12_P_2, 3, 0)
    }

    @Test fun packBed3pAll() {
        val expected = BedEntry("chr1", 10, 30, "val1\t4.55")
        assertEquals(expected, BED_ENTRY_12_P_2.pack(fieldsNumber = 3))
        assertEquals(expected.rest.split("\t"), BED_ENTRY_12_P_2.rest(fieldsNumber = 3))
    }

    @Test fun packBed6pAll() {
        val expected = BedEntry("chr1", 10, 30, "be\t5\t+\tval1\t4.55")
        assertEquals(expected, BED_ENTRY_12_P_2.pack(fieldsNumber = 6))
        assertEquals(expected.rest.split("\t"), BED_ENTRY_12_P_2.rest(fieldsNumber = 6))
    }

    @Test fun packBed6p0() = assertPackAndRest(
        BedEntry("chr1", 10, 30, "be\t5\t+"), BED_ENTRY_12_P_2, 6, 0
    )

    @Test fun packBed6p1() = assertPackAndRest(
        BedEntry("chr1", 10, 30, "be\t5\t+\tval1"), BED_ENTRY_12_P_2, 6, 1
    )

    @Test fun packBed6p2() = assertPackAndRest(
        BedEntry("chr1", 10, 30, "be\t5\t+\tval1\t4.55"), BED_ENTRY_12_P_2, 6, 2
    )

    @Test fun packBed9p2() = assertPackAndRest(
        BedEntry("chr1", 10, 30, "be\t5\t+\t15\t25\t15,16,17\tval1\t4.55"), BED_ENTRY_12_P_2,
        9, 2
    )

    @Test fun packBed12p0() = assertPackAndRest(
        BedEntry("chr1", 10, 30, "be\t5\t+\t15\t25\t15,16,17\t2\t4,5\t11,20"), BED_ENTRY_12_P_2,
        12, 0
    )

    @Test fun packBed12p2() = assertPackAndRest(
        BedEntry("chr1", 10, 30, "be\t5\t+\t15\t25\t15,16,17\t2\t4,5\t11,20\tval1\t4.55"),
        BED_ENTRY_12_P_2,
        12, 2
    )

    @Test(expected = IllegalStateException::class)
    fun packBed2() {
        BED_ENTRY_12_P_2.pack(fieldsNumber = 2)
    }

    @Test(expected = IllegalStateException::class)
    fun packBed13() {
        BED_ENTRY_12_P_2.pack(fieldsNumber = 13)
    }

    @Test fun packBedCustomDelimiter() {
        assertEquals(BedEntry("chr1", 10, 30, "be;5;+;val1;4.55"),
            BED_ENTRY_12_P_2.pack(fieldsNumber = 6, extraFieldsNumber = 2, delimiter = ';'))
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
        val e4 = ExtendedBedEntry("chr1", 10, 30, itemRgb = 0)
        assertPackAndRest(
            BedEntry("chr1", 10, 30, ".\t0\t.\t0\t0\t0"), e4, 9, 0
        )
    }

    @Test fun packNoBlocks() {
        val e3 = ExtendedBedEntry("chr1", 10, 30)
        assertPackAndRest(
            BedEntry("chr1", 10, 30, ".\t0\t.\t0\t0\t0\t0\t.\t."), e3,
            12, 0
        )
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
        assertEquals(expected, actual)
    }

    @Test fun unpackBed3as12() {
        val bedEntry = BedEntry("chr1", 1, 100, "")
        try {
            bedEntry.unpack(fieldsNumber = 12)
        } catch (e: BedEntryUnpackException) {
            assertEquals(3, e.fieldIdx, "Missing field index is wrong")
            return
        }
        assertTrue(false, "bed3 parsed as bed12 without throwing")
    }

    @Test fun unpackBed6as12() {
        val bedEntry = BedEntry("chr1", 1, 100, ".\t4\t+")
        try {
            bedEntry.unpack(fieldsNumber = 12)
        } catch (e: BedEntryUnpackException) {
            assertEquals(6, e.fieldIdx, "Missing field index is wrong")
            return
        }
        assertTrue(false, "bed6 parsed as bed12 without throwing")
    }

    @Test fun unpackBed12as6() {
        val bedEntry = BedEntry("chr1", 1, 100, "be\t5\t+\t15\t25\t15,16,17\t2\t4,5\t11,20")
        val actual = bedEntry.unpack(fieldsNumber = 6)
        assertEquals(
            ExtendedBedEntry(
                "chr1", 1, 100, "be", 5, '+',
                extraFields = arrayOf("15", "25", "15,16,17", "2", "4,5", "11,20")
            ),
            actual
        )
    }

    @Test fun unpackBed3p0() {
        val bedEntry = BedEntry("chr1", 1, 100, ".\t4\t+\t34.56398\t-1.00000\t4.91755\t240")
        assertEquals(
            ExtendedBedEntry("chr1", 1, 100), bedEntry.unpack(fieldsNumber = 3, extraFieldsNumber = 0)
        )
    }

    @Test fun unpackBed3p4Partial() {
        val bedEntry = BedEntry("chr1", 1, 100, ".\t4\t+\t34.56398\t-1.00000\t4.91755\t240")
        assertEquals(
            ExtendedBedEntry("chr1", 1, 100, extraFields = arrayOf(".", "4", "+", "34.56398")),
            bedEntry.unpack(fieldsNumber = 3, extraFieldsNumber = 4)
        )
    }

    @Test fun unpackBed3p4() {
        val bedEntry = BedEntry("chr1", 1, 100, "34.56398\t-1.00000\t4.91755\t240")
        assertEquals(
            ExtendedBedEntry(
                "chr1", 1, 100,
                extraFields = arrayOf("34.56398", "-1.00000", "4.91755", "240")
            ),
            bedEntry.unpack(fieldsNumber = 3, extraFieldsNumber = 4)
        )
    }

    @Test fun unpackBed6p4() {
        val bedEntry = BedEntry("chr1", 1, 100, ".\t4\t+\t34.56398\t-1.00000\t4.91755\t240")
        assertEquals(
            ExtendedBedEntry(
                "chr1", 1, 100, ".", 4, '+',
                extraFields = arrayOf("34.56398", "-1.00000", "4.91755", "240")
            ),
            bedEntry.unpack(fieldsNumber = 6, extraFieldsNumber = 4)
        )
    }

    @Test fun unpackBed6p4IntScore() {
        val bedEntry = BedEntry("chr1", 1, 100, ".\t40000\t+\t34.56398\t-1.00000\t4.91755\t240")
        assertEquals(
            ExtendedBedEntry(
                "chr1", 1, 100, ".", 40000, '+',
                extraFields = arrayOf("34.56398", "-1.00000", "4.91755", "240")
            ),
            bedEntry.unpack(fieldsNumber = 6, extraFieldsNumber = 4)
        )
    }

    @Test fun unpackBedEmptyName() {
        val bedEntry = BedEntry("chr1", 1, 100, "\t4\t+")
        assertEquals(
            ExtendedBedEntry("chr1", 1, 100, ".", 4, '+'),
            bedEntry.unpack(fieldsNumber = 6)
        )
    }

    @Test fun unpackBedEmptyExtraFields1() {
        val bedEntry = BedEntry("chr1", 1, 100, "")
        assertEquals(
            ExtendedBedEntry("chr1", 1, 100), bedEntry.unpack(fieldsNumber = 3)
        )
    }

    @Test fun unpackBedEmptyExtraFields2() {
        val bedEntry = BedEntry("chr1", 1, 100, "foo\t")
        assertEquals(
            ExtendedBedEntry("chr1", 1, 100, "foo"), bedEntry.unpack(fieldsNumber = 4)
        )
    }

    @Test fun unpackMoreExtraFieldsThanNeeded() {
        val bedEntry = BedEntry("chr1", 1, 100, ".\t4\t+\t34.56398\t-1.00000")
        assertEquals(
            ExtendedBedEntry(
                "chr1", 1, 100, ".", 4, '+',
                extraFields = arrayOf("34.56398", "-1.00000")
            ),
            bedEntry.unpack(fieldsNumber = 6, extraFieldsNumber = 4)
        )
    }

    @Test fun unpackLessExtraFieldsThanNeeded() {
        val bedEntry = BedEntry("chr1", 1, 100, ".\t4\t+")
        assertEquals(
            ExtendedBedEntry("chr1", 1, 100, ".", extraFields = arrayOf("4", "+")),
            bedEntry.unpack(fieldsNumber = 4, extraFieldsNumber = 4)
        )
    }
    @Test fun unpackNoExtraFieldsWhenNeeded() {
        val bedEntry = BedEntry("chr1", 1, 100, "\t4\t+")
        assertEquals(
            ExtendedBedEntry("chr1", 1, 100, ".", 4, '+'),
            bedEntry.unpack(fieldsNumber = 6, extraFieldsNumber = 1)
        )
    }

    @Test fun unpackBed4pAll() {
        val bedEntry = BedEntry("chr1", 1, 100, "foo\t34.56398\t-1.00000\t4.91755\t240")
        assertEquals(
            ExtendedBedEntry(
                "chr1", 1, 100, "foo",
                extraFields = arrayOf("34.56398", "-1.00000", "4.91755", "240")
            ),
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

    @Test fun unpackBed3p4CustomDelimiter() {
        val bedEntry = BedEntry("chr1", 1, 100, "34.56398;-1.00000;4.91755;240")
        assertEquals(
            ExtendedBedEntry(
                "chr1", 1, 100,
                extraFields = arrayOf("34.56398", "-1.00000", "4.91755", "240")
            ),
            bedEntry.unpack(fieldsNumber = 3, extraFieldsNumber = 4, delimiter = ';')
        )
    }

    @Test fun unpackBed3p4OmitEmptyStrings() {
        val bedEntry = BedEntry("chr1", 1, 100, "34.56398    -1.00000    4.91755    240")
        assertEquals(
            ExtendedBedEntry(
                "chr1", 1, 100,
                extraFields = arrayOf("34.56398", "-1.00000", "4.91755", "240")
            ),
            bedEntry.unpack(
                fieldsNumber = 3, extraFieldsNumber = 4, delimiter = ' ', omitEmptyStrings = true
            )
        )
    }

    @Test fun unpackBedDotDefaultValues() {
        assertEquals(
            ExtendedBedEntry("chr1", 1, 100),
            BedEntry("chr1", 1, 100, ".\t.\t.\t.\t.\t.\t.\t.\t.").unpack()
        )

        assertEquals(
            ExtendedBedEntry("chr1", 1, 100, extraFields = arrayOf(".")),
            BedEntry("chr1", 1, 100, ".\t.\t.\t.\t.\t.\t.\t.\t.\t.").unpack()
        )
        assertEquals(
            ExtendedBedEntry("chr1", 1, 100, extraFields = arrayOf(".", ".")),
            BedEntry("chr1", 1, 100, ".\t.\t.\t.\t.\t.\t.\t.\t.\t.\t.").unpack()
        )
    }

    @Test
    fun getField() = assertGet()

    @Test
    fun getFieldBed3p0() = assertGet(3, 0)

    @Test
    fun getFieldBed3pAll() = assertGet(3)

    @Test
    fun getFieldBed6p0() = assertGet(6, 0)

    @Test
    fun getFieldBed6p1() = assertGet(6, 1)

    @Test
    fun getFieldBed6p2() = assertGet(6, 2)

    @Test
    fun getFieldBed6pAll() = assertGet(6)

    @Test
    fun getFieldBed9p2() = assertGet(9, 2)

    @Test
    fun getFieldBed12p0() = assertGet(12, 0)

    @Test
    fun getFieldBed12p2() = assertGet(12, 2)

    private fun assertGet(fieldsNumber: Int = 12, extraFieldsNumber: Int? = null) {
        val actualFields = (0 until 14).map { BED_ENTRY_12_P_2.getField(it, fieldsNumber, extraFieldsNumber) }
        val realExtraFieldsNumber = extraFieldsNumber ?: 2
        val expectedFields = listOf<Any?>(
            "chr1", 10, 30, "be", 5, '+', 15, 25, Color(15, 16, 17).rgb,
            2, intArrayOf(4, 5), intArrayOf(11, 20)
        ).slice(0 until fieldsNumber).toMutableList()
        expectedFields.addAll(listOf("val1", "4.55").slice(0 until realExtraFieldsNumber))
        // out-of-bounds fields are always null
        (fieldsNumber + realExtraFieldsNumber until 14).forEach { expectedFields.add(null) }
        for (i in 0 until 14) {
            val expectedField = expectedFields[i]
            val actualField = actualFields[i]
            // a special case for IntArray, since assertEquals is not array-friendly
            if (expectedField is IntArray) {
                assertTrue(
                    actualField is IntArray,
                    "Expected IntArray as field $i, got ${actualField?.javaClass ?: "null"}"
                )
                assertEquals(
                    expectedField.toList(), (actualField as IntArray).toList(),
                    "Assertion error when reading field $i"
                )
            } else {
                assertEquals(expectedField, actualField, "Assertion error when reading field $i")
            }
        }
    }

    private fun assertPackAndRest(expected: BedEntry, e: ExtendedBedEntry, fieldsNumber: Byte, extraFieldsNumber: Int) {
        assertEquals(
            expected,
            e.pack(fieldsNumber = fieldsNumber, extraFieldsNumber = extraFieldsNumber)
        )
        val toTypedArray = expected.rest.let {
            if (it.isEmpty()) {
                emptyArray()
            } else {
                it.split("\t").toTypedArray()
            }
        }
        Assert.assertArrayEquals(
            toTypedArray,
            e.rest(fieldsNumber = fieldsNumber, extraFieldsNumber = extraFieldsNumber).toTypedArray()
        )
    }

    companion object {
        val BED_ENTRY_12_P_2 = ExtendedBedEntry(
            "chr1", 10, 30, "be", 5, '+', 15, 25,
            Color(15, 16, 17).rgb, 2, intArrayOf(4, 5), intArrayOf(11, 20),
            arrayOf("val1", "4.55")
        )
    }
}