package org.jetbrains.bio.big

import org.jetbrains.bio.Examples
import org.jetbrains.bio.NamedRomBufferFactoryProvider
import org.jetbrains.bio.read
import org.jetbrains.bio.romFactoryProviderAndPrefetchParams
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull

@RunWith(Parameterized::class)
class ZoomLevelTest(
        private val bfProvider: NamedRomBufferFactoryProvider,
        private val prefetch: Boolean
) {
    @Test
    fun testReductionLevelBB() {
        BigBedFile.read(Examples["example1.bb"].toAbsolutePath(), bfProvider, prefetch).use { bf ->
            assertEquals(bf.zoomLevels[0].reduction, 3911)
            assertEquals(bf.zoomLevels[1].reduction, 39110)
        }
    }

    @Test
    fun testReductionLevelBW() {
        BigWigFile.read(Examples["example2.bw"].toAbsolutePath(), bfProvider, prefetch).use { bf ->
            assertEquals(bf.zoomLevels[0].reduction, 100)
            assertEquals(bf.zoomLevels[1].reduction, 400)
        }
    }

    @Test
    fun testPick() {
        BigBedFile.read(Examples["example1.bb"], bfProvider, prefetch).use { bbf ->
            val zoomLevel = bbf.zoomLevels.pick(5000000)
            assertNotNull(zoomLevel);
            assertEquals(zoomLevel!!.reduction, 3911000)

            // desiredReduction <= 1
            assertNull(bbf.zoomLevels.pick(1))

            // diff = desiredReduction - zoomLevel.reductionLevel < 0
            assertNull(bbf.zoomLevels.pick(1000))
        }
    }

    companion object {
        @Parameterized.Parameters(name = "{0}:{1}")
        @JvmStatic
        fun data() = romFactoryProviderAndPrefetchParams()
    }
}