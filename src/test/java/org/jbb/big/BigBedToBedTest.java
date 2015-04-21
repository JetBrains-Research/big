package org.jbb.big;

import junit.framework.TestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class BigBedToBedTest extends TestCase {
  public void testBigBedToBed() throws Exception {
    final Path inputPath = Examples.get("example1.bb");
    final Path outputPath = Files.createTempFile("out", ".bed");
    BigBedToBed.main(inputPath, outputPath, "", 0, 0, 0);
    try {
      assertTrue(Files.exists(outputPath));
      assertTrue(Files.size(outputPath) > 0);
    } finally {
      Files.deleteIfExists(outputPath);
    }
  }

  public void testBigBedToBedFilterByChromosomeName() throws Exception {
    final Path inputPath = Examples.get("example1.bb");
    final Path outputPath = Files.createTempFile("out", ".bed");
    final int chromStart = 0;
    final int chromEnd = 0;
    final int maxItems = 0;
    // In example1.bb we have only chr21 chromosome
    String chromName = "chr22";
    BigBedToBed.main(inputPath, outputPath, chromName, chromStart, chromEnd, maxItems);
    try {
      assertTrue(Files.exists(outputPath));
      assertFalse(Files.size(outputPath) > 0);
    } finally {
      Files.deleteIfExists(outputPath);
    }
    // This chromosome exist in example bb-file
    chromName = "chr21";
    BigBedToBed.main(inputPath, outputPath, chromName, chromStart, chromEnd, maxItems);
    try {
      assertTrue(Files.exists(outputPath));
      assertTrue(Files.size(outputPath) > 0);
    } finally {
      Files.deleteIfExists(outputPath);
    }
    // Get all chromosome from example file
    chromName = "";
    BigBedToBed.main(inputPath, outputPath, chromName, chromStart, chromEnd, maxItems);
    try {
      assertTrue(Files.exists(outputPath));
      assertTrue(Files.size(outputPath) > 0);
    } finally {
      Files.deleteIfExists(outputPath);
    }
  }

  public void testBigBedToBedRestrictOutput() throws Exception {
    final Path inputPath = Examples.get("example1.bb");
    final Path outputPath = Files.createTempFile("out", ".bed");
    // Params restriction
    // In example1.bb we have only one chromosome
    final String chromName = "chr21";
    int chromStart = 0;
    int chromEnd = 0;
    int maxItems = 10;
    // Check lines count in output file
    BigBedToBed.main(inputPath, outputPath, chromName, chromStart, chromEnd, maxItems);
    try (final Stream<String> lines = Files.lines(outputPath)) {
      assertTrue(Files.exists(outputPath));
      assertTrue(Files.size(outputPath) > 0);
      assertEquals(maxItems, lines.count());
    } finally {
      Files.deleteIfExists(outputPath);
    }
    // Restrict intervals
    chromStart = 9508110;
    chromEnd = 9906613;
    BigBedToBed.main(inputPath, outputPath, chromName, chromStart, chromEnd, maxItems);
    try (final Stream<String> lines = Files.lines(outputPath)) {
      assertTrue(Files.exists(outputPath));
      assertTrue(Files.size(outputPath) > 0);
      assertEquals(5, lines.count());
//      Files.copy(outputPath, System.out);
    } finally {
      Files.deleteIfExists(outputPath);
    }

    chromStart = 9508110;
    chromEnd = 9906612;
    BigBedToBed.main(inputPath, outputPath, chromName, chromStart, chromEnd, maxItems);
    try (final Stream<String> lines = Files.lines(outputPath)) {
      assertTrue(Files.exists(outputPath));
      assertTrue(Files.size(outputPath) > 0);
      assertEquals(4, lines.count());
    } finally {
      Files.deleteIfExists(outputPath);
    }

    chromStart = 9508110;
    chromEnd = 9906614;
    BigBedToBed.main(inputPath, outputPath, chromName, chromStart, chromEnd, maxItems);
    try (final Stream<String> lines = Files.lines(outputPath)) {
      assertTrue(Files.exists(outputPath));
      assertTrue(Files.size(outputPath) > 0);
      assertEquals(5, lines.count());
    } finally {
      Files.deleteIfExists(outputPath);
    }

    chromStart = 9508110;
    chromEnd = 9903230;
    maxItems = 3;
    BigBedToBed.main(inputPath, outputPath, chromName, chromStart, chromEnd, maxItems);
    try (final Stream<String> lines = Files.lines(outputPath)) {
      assertTrue(Files.exists(outputPath));
      assertTrue(Files.size(outputPath) > 0);
      assertEquals(3, lines.count());
    } finally {
      Files.deleteIfExists(outputPath);
    }
  }
}