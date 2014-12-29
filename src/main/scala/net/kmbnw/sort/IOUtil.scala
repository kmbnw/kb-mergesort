/*
 * Copyright 2010-2011 Krysta M Bouzek
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the version 3 of the GNU Lesser General Public License
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.kmbnw.sort

import java.io.{File, BufferedReader, FileReader, Closeable, Flushable}
import java.io.{BufferedWriter, FileWriter, PrintWriter}
import scala.collection.mutable.{Map, LinkedHashMap}

/**
 * A collection of general purpose stream I/O helper methods.
 */
object IOUtil {
  /**
   * Reads the next line from the stream.  Closes the stream if there are no
   * more values.
   * @param stream The stream to read
   * @return The value read; null if the stream was empty.
   */
  def readMaybe(stream: BufferedReader): String = {
    val next = stream.readLine
    if (null == next) { safeClose(stream) }
    next
  }

  /**
   * Turns a list of files into a linked map of file name to buffered reader.
   * @param files The list of files to map.
   * @return A map of (file name -&gt; open BufferedReader) for that file.  If files is empty, this
   * returns an empty map.
   */
  def toStreams(files: Seq[File]): Map[String, BufferedReader]= {
    val filemap = LinkedHashMap[String, BufferedReader]()
    if (files.isEmpty) { filemap  }
    else {
      files.foreach({ f =>
        filemap += {f.getName -> new BufferedReader(new FileReader(f))}
      })
      filemap
    }
  }

  /**
   * Attempts a close on the closeable object, printing any exceptions.  Does nothing if
   * closeable is null.
   */
  def safeClose(closeable: Closeable): Unit = {
    if (null != closeable) {
      try { closeable.close }
      catch { case t: Throwable => t.printStackTrace }
    }
  }

  /**
   * Attempts a flush on the flushable object, printing any exceptions.  Does nothing if
   * flushable is null.
   */
  def safeFlush(flushable: Flushable): Unit = {
    if (null != flushable) {
      try { flushable.flush() }
      catch { case t: Throwable => t.printStackTrace }
    }
  }

  /**
   * Opens a buffered file reader, passing it to the callback.
   * @param file The file to open for reading.
   * @param callback The function that will be passed a BufferedReader to work with
   */
  def withInput[T](file: File, callback: (BufferedReader) => T) = {
    var fr: FileReader = null
    var br: BufferedReader = null

    try {
      fr = new FileReader(file)
      br = new BufferedReader(fr)
      callback(br)
    } finally {
      safeClose(fr)
      safeClose(br)
    }
  }

  /**
   * Opens a buffered writer, passing it to the callback.
   * @param file The file to open for writing.
   * @param callback The function that will be passed a BufferedWriter to work with
   */
  def withOutput[T](file: File, callback: (PrintWriter) => T) = {
    var fw: FileWriter = null
    var bw: BufferedWriter = null
    var pw: PrintWriter = null

    try {
      fw = new FileWriter(file)
      bw = new BufferedWriter(fw)
      pw = new PrintWriter(fw, false)
      callback(pw)
    } finally {
      safeFlush(pw)
      safeFlush(bw)
      safeClose(pw)
      safeClose(bw)
      safeClose(fw)
    }
  }
}
