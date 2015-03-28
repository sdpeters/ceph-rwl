/*
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */
package com.ceph.fs;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

/*
 * There are several examples of embedding native bits in a JAR out in the
 * wild. Many are complicated because they try to cover a wide variety of
 * platforms. RocksDB Java covers Linux and MacOS and is easy to grok. Our
 * approach is a simplified version of the RocksDB loader. Additionally, we
 * only need Linux support right now.
 *
 * The basic idea is to first try the normal java.library.path, and then
 * fallback to the JAR file.
 */
class CephNativeLoader {

  private static final CephNativeLoader instance = new CephNativeLoader();
  private static boolean initialized = false;

  // Linux specific
  private static final String sharedLibraryName = "cephfs_jni";
  private static final String jniLibraryFilename = "libcephfs_jni.so";
  private static final String tempFilePrefix = "libcephfsjni";
  private static final String tempFileSuffix = ".so";

  static CephNativeLoader getInstance() {
    return instance;
  }

  /**
   * Try default path, and then fallback to jar.
   */
  synchronized void loadLibrary() throws IOException {
    try {
      System.loadLibrary(sharedLibraryName);
    } catch (final UnsatisfiedLinkError ule1) {
      loadLibraryFromJar();
    }
    CephMount.native_initialize();
    initialized = true;
  }

  /**
   * Extract the JNI bits to a temp file and load that.
   */
  private synchronized void loadLibraryFromJar() throws IOException {
    if (!initialized) {
      // where we will stash the extracted library bits
      final File temp = File.createTempFile(tempFilePrefix, tempFileSuffix);
      if (!temp.exists()) {
        throw new RuntimeException("File " + temp.getAbsolutePath() + " does not exist.");
      } else {
        temp.deleteOnExit();
      }

      // look inside the jar and grab the library
      InputStream is = null;
      try {
        is = getClass().getClassLoader().getResourceAsStream(jniLibraryFilename);
        if (is == null) {
          throw new RuntimeException(jniLibraryFilename + " was not found inside the jar");
        } else {
          Files.copy(is, temp.toPath(), StandardCopyOption.REPLACE_EXISTING);
        }
      } finally {
        if (is != null)
          is.close();
      }
      System.load(temp.getAbsolutePath());
    }
  }
}
