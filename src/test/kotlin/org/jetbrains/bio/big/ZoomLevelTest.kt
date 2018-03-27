package org.jetbrains.bio.big

import org.jetbrains.bio.Examples
import org.jetbrains.bio.RomBufferFactoryProvider
import org.jetbrains.bio.read
import org.jetbrains.bio.romFactoryProviders
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

@RunWith(Parameterized::class)
class ZoomLevelTest(private val bfProvider: RomBufferFactoryProvider) {
    @Test fun testReductionLevel() {
        BigBedFile.read(Examples.get("example1.bb").toAbsolutePath(), bfProvider).use { bbf ->
            assertEquals(bbf.zoomLevels[0].reduction, 3911)
            assertEquals(bbf.zoomLevels[1].reduction, 39110)
        }
    }

    @Test fun testPick() {
        BigBedFile.read(Examples["example1.bb"], bfProvider).use { bbf ->
            val zoomLevel = bbf.zoomLevels.pick(5000000);
            assertNotNull(zoomLevel);
            assertEquals(zoomLevel!!.reduction, 3911000)

            // desiredReduction <= 1
            assertNull(bbf.zoomLevels.pick(1))

            // diff = desiredReduction - zoomLevel.reductionLevel < 0
            assertNull(bbf.zoomLevels.pick(1000))
        }
    }

    companion object {
        @Parameterized.Parameters(name = "{0}")
        @JvmStatic fun data() = romFactoryProviders().map { arrayOf<Any>(it) }
    }
}