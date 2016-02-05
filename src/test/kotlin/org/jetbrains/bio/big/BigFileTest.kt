package org.jetbrains.bio.big

import org.jetbrains.bio.Examples
import org.jetbrains.bio.withTempFile
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class BigFileTest {
    @Test fun testReadHeader() {
        BigBedFile.read(Examples["example1.bb"]).use { bbf ->
            val header = bbf.header
            assertEquals(1, header.version)
            assertEquals(5, header.zoomLevelCount)
            assertEquals(3, header.fieldCount)
            assertEquals(3, header.definedFieldCount)
            assertEquals(0, header.uncompressBufSize)
        }
    }

    @Test fun testZoomPartitioning() {
        // In theory we can use either WIG or BED, but WIG is just simpler.
        withTempFile("example2", ".bw") { path ->
            BigWigFile.read(Examples["example2.bw"]).use { bwf ->
                val (name, _chromIx, size) =
                        bwf.bPlusTree.traverse(bwf.input.mapped).first()
                BigWigFile.write(bwf.query(name).take(32).toList(), listOf(name to size), path)
            }

            BigWigFile.read(path).use { bwf ->
                val (_name, chromIx, size) =
                        bwf.bPlusTree.traverse(bwf.input.mapped).first()
                val query = Interval(chromIx, 0, size)
                for ((reduction, _dataOffset, indexOffset) in bwf.zoomLevels) {
                    if (reduction == 0) {
                        break
                    }

                    val zRTree = RTreeIndex.read(bwf.input.mapped, indexOffset)
                    val blocks = zRTree.findOverlappingBlocks(bwf.input, query).toList()
                    for (i in blocks.indices) {
                        for (j in i + 1..blocks.size - 1) {
                            assertFalse(blocks[i].interval intersects blocks[j].interval)
                        }
                    }
                }
            }
        }
    }
}