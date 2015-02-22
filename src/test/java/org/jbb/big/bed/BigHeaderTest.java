package org.jbb.big.bed;

import junit.framework.TestCase;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by pacahon on 21.02.15.
 */
public class BigHeaderTest extends TestCase {
  public void testParseBigHeader() {
    // http://genome.ucsc.edu/goldenpath/help/bigBed.html
    URL url = getClass().getClassLoader().getResource("example1.bb");
    Path p = Paths.get(url.getPath());
    if (Files.exists(p)) {
      try {
        BigHeader bigHeader = BigHeader.parse(p);
        assertTrue(bigHeader.version > 0);
        assertTrue(bigHeader.zoomLevels > 0);
        assertTrue(bigHeader.chromTreeOffset > 0);
        assertTrue(bigHeader.unzoomedDataOffset > 0);
        assertTrue(bigHeader.unzoomedIndexOffset > 0);
        assertTrue(bigHeader.fieldCount > 0);
        assertTrue(bigHeader.definedFieldCount > 0);
        assertTrue(bigHeader.asOffset > 0);
        assertTrue(bigHeader.totalSummaryOffset > 0);
        assertTrue(bigHeader.uncompressBufSize > 0);
        assertTrue(bigHeader.extensionOffset > 0);
      } catch (IOException ioe) {
        fail("IOException: " + ioe.getMessage());
      }
    } else {
      fail("Resourse file not found");
    }

  }

}
