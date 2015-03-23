package org.jbb.big;

import java.util.Objects;

/**
 * A pair of two things. Simple as that.
 *
 * @author Sergei Lebedev
 * @since 23/03/15
 */
class Pair<A, B> {
  public final A first;
  public final B second;

  protected Pair(final A first, final B second) {
    this.first = Objects.requireNonNull(first);
    this.second = Objects.requireNonNull(second);
  }

  public static <A, B> Pair<A, B> create(final A first, final B second) {
    return new Pair<>(first, second);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj)
      return true;
    if (!(obj instanceof Pair))
      return false;

    final Pair other = (Pair) obj;
    return Objects.equals(first, other.first) &&
           Objects.equals(second, other.second);
  }

  @Override
  public int hashCode() {
    return Objects.hash(first, second);
  }

  @Override
  public String toString() {
    return "(" + first + ", " + second + ')';
  }
}