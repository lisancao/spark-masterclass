package jdocs;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/** One listed file: everything the planner knows without opening it. Serializable: it travels in tasks. */
public final class FileEntry implements Serializable {
  public final String path, name, kind;
  public final long size, modifiedMicros;

  FileEntry(Path p, BasicFileAttributes a) {
    this.path = p.toAbsolutePath().normalize().toString();
    this.name = p.getFileName().toString();
    this.kind = kindOf(name);
    this.size = a.size();
    this.modifiedMicros = a.lastModifiedTime().to(TimeUnit.MICROSECONDS);
  }

  static final Map<String, String> KINDS = Map.of(
      ".png", "image", ".wav", "audio", ".pdf", "pdf", ".eml", "email", ".md", "text",
      ".txt", "text", ".log", "log", ".json", "json", ".xml", "xml");

  static String kindOf(String name) {
    int dot = name.lastIndexOf('.');
    return dot < 0 ? "other" : KINDS.getOrDefault(name.substring(dot).toLowerCase(Locale.ROOT), "other");
  }

  /** Every regular file under root, sorted by path. Reads file-system metadata only. */
  static List<FileEntry> list(String root) {
    Path dir = Paths.get(root);
    if (!Files.isDirectory(dir))
      throw new IllegalArgumentException("option 'path' must be an existing folder, got '" + root + "'");
    List<FileEntry> out = new ArrayList<>();
    try (Stream<Path> s = Files.walk(dir)) {
      for (Path p : (Iterable<Path>) s::iterator) {
        BasicFileAttributes a = Files.readAttributes(p, BasicFileAttributes.class);
        if (a.isRegularFile()) out.add(new FileEntry(p, a));
      }
    } catch (IOException e) { throw new UncheckedIOException(e); }
    out.sort(Comparator.comparing(f -> f.path));
    return out;
  }

  /** PNG width and height from the first 24 bytes, or null if the bytes are not a PNG. */
  static int[] pngSize(String path) throws IOException {
    Audit.OPENED.incrementAndGet();
    byte[] h = new byte[24];
    int n;
    try (InputStream in = Files.newInputStream(Paths.get(path))) { n = in.readNBytes(h, 0, 24); }
    byte[] sig = {(byte) 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'};
    if (n < 24 || !Arrays.equals(Arrays.copyOf(h, 8), sig)
        || h[12] != 'I' || h[13] != 'H' || h[14] != 'D' || h[15] != 'R') return null;
    ByteBuffer b = ByteBuffer.wrap(h, 16, 8);   // big-endian, like the PNG spec
    return new int[] {b.getInt(), b.getInt()};
  }
}
