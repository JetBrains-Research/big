package org.jbb.big;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author pacahon
 * @since 04.04.15
 */
public class Examples {

  public static Path get(final String name) throws URISyntaxException {
    final URL url = Examples.class.getClassLoader().getResource(name);
    if (url == null) {
      throw new IllegalStateException("resource not found");
    }
    return Paths.get(url.toURI()).toFile().toPath();
  }

}


