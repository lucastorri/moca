package com.github.lucastorri.moca.browser.webkit.net

import java.io.{InputStream, OutputStream}

class InterceptedInputStream(in: InputStream, out: OutputStream) extends InputStream {

  override def available(): Int =
    in.available()

  override def mark(limit: Int): Unit =
    in.mark(limit)

  override def skip(n: Long): Long =
    in.skip(n)

  override def markSupported(): Boolean =
    in.markSupported()

  override def read(): Int = {
    val b = in.read()
    if (b > 0) out.write(b)
    b
  }

  override def read(b: Array[Byte]): Int = {
    val n = in.read(b)
    if (n > 0) out.write(b, 0, n)
    n
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    val n = in.read(b, off, len)
    if (n > 0) out.write(b, off, n)
    n
  }

  override def reset(): Unit =
    in.reset()

  override def close(): Unit = {
    out.flush()
    out.close()
    in.close()
  }

}
