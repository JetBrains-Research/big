package org.jbb.big;

import com.sun.istack.internal.Nullable;

import junit.framework.TestCase;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author Sergey Zherevchuk
 */
public class BigHeaderTest extends TestCase {
  public void testParseBigHeader() throws Exception {
    // http://genome.ucsc.edu/goldenpath/help/bigBed.html
    final @Nullable URL url = getClass().getClassLoader().getResource("example1.bb");
    assert url != null : "resource not found";
    final Path p = Paths.get(url.getPath());
      final BigHeader bigHeader = BigHeader.parse(p);
      assertTrue(bigHeader.version > 0);
      assertTrue(bigHeader.zoomLevels > 0);
      assertTrue(bigHeader.chromTreeOffset >= 0);
      assertTrue(bigHeader.unzoomedDataOffset >= 0);
      assertTrue(bigHeader.unzoomedIndexOffset >= 0);
      assertTrue(bigHeader.fieldCount > 0);
      assertTrue(bigHeader.definedFieldCount > 0);
      assertTrue(bigHeader.asOffset >= 0);
      assertTrue(bigHeader.totalSummaryOffset >= 0);
      assertTrue(bigHeader.uncompressBufSize >= 0);
      assertTrue(bigHeader.extensionOffset >= 0);
  }

}
