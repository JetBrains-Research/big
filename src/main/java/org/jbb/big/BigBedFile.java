package org.jbb.big;

import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

/**
 * Just like BED only BIGGER.
 *
 * @author Sergei Lebedev
 * @date 11/04/15
 */
public class BigBedFile extends BigFile {
  public static BigBedFile parse(final Path path) throws IOException {
    return new BigBedFile(path);
  }

  protected BigBedFile(final Path path) throws IOException {
    super(path);
  }

  public List<BedData> query(final String chromName, final int startOffset, final int endOffset)
      throws IOException {
    return query(chromName, startOffset, endOffset, -1);
  }

  public List<BedData> query(final String chromName, final int startOffset, final int endOffset,
                             final int maxItems)
      throws IOException {
    final Optional<BPlusLeaf> bpl = header.bPlusTree.find(handle, chromName);
    if (bpl.isPresent()) {
      final int chromIx = bpl.get().id;
      final RTreeInterval target = RTreeInterval.of(chromIx, startOffset, endOffset);
      return header.rTree.findOverlaps(handle, target, maxItems);
    } else {
      return ImmutableList.of();
    }
  }
}
