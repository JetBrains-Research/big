package org.jbb.big;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * A common superclass for Big files.
 *
 * @author Sergei Lebedev
 * @author Sergey Zherevchuk
 * @since 11/04/15
 */
abstract class BigFile<T> implements Closeable, AutoCloseable {

  @VisibleForTesting
  protected static class Header {

    static Header parse(final SeekableDataInput s, final int magic) throws IOException {
      s.guess(magic);

      final short version = s.readShort();
      final short zoomLevels = s.readShort();
      final long chromTreeOffset = s.readLong();
      final long unzoomedDataOffset = s.readLong(); // fullDataOffset in supplemental info
      final long unzoomedIndexOffset = s.readLong();
      final short fieldCount = s.readShort();
      final short definedFieldCount = s.readShort();
      final long asOffset = s.readLong();
      final long totalSummaryOffset = s.readLong();
      final int uncompressBufSize = s.readInt();
      final long reserved = s.readLong();  // currently 0.

      if (uncompressBufSize > 0) {
        throw new IllegalStateException("data compression is not supported");
      }

      if (reserved != 0) {
        throw new IllegalStateException("header extensions are not supported");
      }

      final List<ZoomLevel> zoomLevelList = new ArrayList<>();
      for (int i = 0; i < zoomLevels; i++) {
        zoomLevelList.add(new ZoomLevel(s.readInt(), s.readInt(),
                                        s.readLong(), s.readLong()));
      }

      final BPlusTree bpt = BPlusTree.read(s, chromTreeOffset);
      final RTreeIndex rti = RTreeIndex.read(s, unzoomedIndexOffset);
      return new Header(s.order(), version, zoomLevels,
                        unzoomedDataOffset, fieldCount, definedFieldCount,
                        asOffset, totalSummaryOffset, uncompressBufSize, zoomLevelList,
                        bpt, rti);
    }

    public final ByteOrder byteOrder;
    public final short version;
    public final short zoomLevels;
    public final long unzoomedDataOffset;  // FullDataOffset in table 5
    public final short fieldCount;
    public final short definedFieldCount;
    public final long asOffset;
    public final long totalSummaryOffset;
    public final int uncompressBufSize;
    public final List<ZoomLevel> zoomLevelList;
    public final BPlusTree bPlusTree;
    public final RTreeIndex rTree;

    protected Header(final ByteOrder byteOrder, final short version,
                     final short zoomLevels, final long unzoomedDataOffset,
                     final short fieldCount, final short definedFieldCount, final long asOffset,
                     final long totalSummaryOffset, final int uncompressBufSize,
                     final List<ZoomLevel> zoomLevelList,
                     final BPlusTree bPlusTree, final RTreeIndex rTree) {
      this.byteOrder = byteOrder;
      this.version = version;
      this.zoomLevels = zoomLevels;
      this.unzoomedDataOffset = unzoomedDataOffset;
      this.fieldCount = fieldCount;
      this.definedFieldCount = definedFieldCount;
      this.asOffset = asOffset;
      this.totalSummaryOffset = totalSummaryOffset;
      this.uncompressBufSize = uncompressBufSize;
      this.zoomLevelList = zoomLevelList;
      this.bPlusTree = bPlusTree;
      this.rTree = rTree;
    }
  }

  // XXX maybe we should make it a DataIO instead of separate
  // Input/Output classes?
  protected final SeekableDataInput handle;
  protected final Header header;

  protected BigFile(final Path path) throws IOException {
    this.handle = SeekableDataInput.of(path);
    this.header = BigFile.Header.parse(handle, getHeaderMagic());
  }

  public Set<String> chromosomes() throws IOException {
    final ImmutableSet.Builder<String> b = ImmutableSet.builder();
    header.bPlusTree.traverse(handle, bpl -> b.add(bpl.key));
    return b.build();
  }

  /**
   * Defaults {@code maxItems} to {@code 0}.
   */
  public List<T> query(final String chromName, final int startOffset, final int endOffset)
      throws IOException {
    return query(chromName, startOffset, endOffset, 0);
  }

  /**
   * Queries an R+-tree.
   *
   * @param chromName human-readable chromosome name, e.g. {@code "chr9"}.
   * @param startOffset 0-based start offset (inclusive).
   * @param endOffset 0-based end offset (exclusive), if 0 than the whole
   *                  chromosome is used.
   * @param maxItems upper bound on the number of items to return.
   * @return a list of intervals overlapping the query.
   * @throws IOException if the underlying {@link SeekableDataInput} does so.
   */
  public List<T> query(final String chromName, final int startOffset, final int endOffset,
                       final int maxItems)
      throws IOException {
    final Optional<BPlusLeaf> bpl = header.bPlusTree.find(handle, chromName);
    if (bpl.isPresent()) {
      final int chromIx = bpl.get().id;
      final RTreeInterval query = RTreeInterval.of(
          chromIx, startOffset, endOffset == 0 ? bpl.get().size : endOffset);
      return queryInternal(query, maxItems);
    } else {
      return ImmutableList.of();
    }
  }

  public abstract int getHeaderMagic();

  protected abstract List<T> queryInternal(RTreeInterval query, int maxItems)
      throws IOException;

  @Override
  public void close() throws IOException {
    handle.close();
  }
}
