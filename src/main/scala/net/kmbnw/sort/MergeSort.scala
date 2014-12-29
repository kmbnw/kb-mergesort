/*
 * Copyright 2010 Krysta M Bouzek
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

import java.io.{File, BufferedReader, FileReader, Closeable}
import java.io.{BufferedWriter, FileWriter}
import scala.util.Sorting
import scala.collection.immutable.Queue
import java.util.regex.Pattern
import scala.collection.mutable.{Map, LinkedHashMap, HashMap, PriorityQueue}
import scala.collection.mutable.{ArrayBuffer}

// Import I/O helpers
import net.kmbnw.sort.IOUtil._

/**
 * A merge sort for sorting large files.  Meant to work like *NIX sort (although it is
 * currently only supports a small subset of sort's options).  Inspired by the algorithm,
 * but not the Java implementation, from<p>
 * http://www.codeodor.com/index.cfm/2007/5/10/Sorting-really-BIG-files/1194</p>.
 * Here's the algorithm from that page:<br>
 *    1. Until finished reading the large file<br>
 *      1. Read a large chunk of the file into memory (large enough so that you get <br>
 *         a lot of records, but small enough such that it will comfortably fit
 *         into memory).<br>
 *      2. Sort those records in memory.<br>
 *      3. Write them to a (new) file<br>
 *    2. Open each of the files you created above<br>
 *    3. Read the top record from each file<br>
 *    4. Until no record exists in any of the files (or until you have read the
 *      entirety of every file)<br>
 *      1. Write the smallest record to the sorted file<br>
 *      2. Read the next record from the file that had the smallest record<br>
 * <br>
 * The above is (I believe) a K-way external merge sort.  The key difference from the
 * tape sorts in the literature is that the "runs" are written to separate files.
 * Thinking about "rewinding" a tape or reading "runs" from a set of tapes makes it
 * harder to reason about in file-land.<br>
 * I made one adjustment to the above algorithm; this implementation allows
 * many temporary files for the initial sort, so it uses multiple merge passes rather
 * than one big one.<br>
 * I also used the following as references for understanding external merge sorts:
 * <ul>
 * <li>"The Algorithm Design Manual", 2nd ed., by Steven S. Skiena</li>
 * <li>"Algorithms", 1984 reprinting, by Robert Sedgewick</li>
 * <li>"Data Structures and Problem Solving using Java", 2nd ed., by Mark Allen Weiss</li>
 * </ul><br>
 * Any errors in this implementation come from my (mis)understanding of external merge
 * sort and should not reflect poorly on the above authors.
 * @author Krysta M Bouzek
 * @param sortSz The maximum number of records to sort at once.
 * @param mergeSz The maximum number of files to merge at once.
 * @param sep The separator to use to split the input lines.  Will be auto-escaped.
 * @param cols The column indices to sort on.
 * @param reverse True to reverse the sort.
 */

/**
 * Data structure for merging via the priority queue.
 */
private class PQStruct(
  val filename: String,
  val tstamp: Int,
  val key: String,
  val line: String) { }

/**
 * The main merge sort class.
 */
class MergeSort(
  cols: Array[Int],
  sortSz: Int = 10000,
  mergeSz: Int = 32,
  separator: String = " ",
  reverse: Boolean = false,
  stable: Boolean = false) {
  private val sep = Pattern.quote(separator)

  // NOTE the sort() and merge() methods do apparently unnecessary work trying
  // to figure out if they only have to write one file and writing directly to the
  // destination if so.  This is to get around the limitation that renameTo does
  // not work across partitions

  private val tmpDir = new File(System.getProperty("java.io.tmpdir"))
  if (sortSz < 2) {
    throw new IllegalArgumentException("Max records to sort must be greater than one.")
  }

  if (mergeSz < 2) {
    throw new IllegalArgumentException("Max files to merge must be greater than one.")
  }

  /**
   * An ordering for (concatenated keys, line read from file).  This is used instead of
   * just keeping an array version of a given line from a file in order to avoid a lot
   * of calls to mkString.
   */
  private val keyOrder: Ordering[(String, String)] =
    Ordering.fromLessThan(if (reverse) { _._1 > _._1 } else { _._1 < _._1 })

  /**
   * An ordering on a PQStruct.  Scala's priority queues use a max-heap ordering,
   * so for an ascending sort, this uses greater-than rather than less-than.
   */
  private val filenameOrder: Ordering[PQStruct] = {
    Ordering.fromLessThan(
      if (stable) {
        if (reverse) {
          (a, b) => {
            if (a.key == b.key) { a.tstamp < b.tstamp } else { a.key < b.key }
          }
        } else {
          // TODO should this be >= instead of > ?
          (a, b) => {
            if (a.key == b.key) { a.tstamp > b.tstamp } else { a.key > b.key }
          }
        }
      } else {
        if (reverse) { _.key < _.key } else { _.key > _.key }
      })
  }

  /**
   * Sorts an array of elements in-memory and writes it to a file.
   * @param buf The buffer to write.
   * @param count The number of elements in the buffer to write.
   * @param dstOpt If this is not None, then the file written to will be the user
   * requested destination file.  Otherwise a temp file will be written to.  This
   * exists to get around Java's limitation on renaming files across partitions.
   * @return The file written.
   */
  private def write(
    buf: Array[(String, String)],
    count: Int,
    dstOpt: Option[File]): File = {
    val newBuf = buf.take(count)
    if (stable) {
      Sorting.stableSort(newBuf)(manifest[(String, String)], keyOrder)
    } else {
      Sorting.quickSort(newBuf)(keyOrder)
    }
    val written = fileToWrite(dstOpt)
    withOutput(written, pw => {
      newBuf.foreach(line => {
        pw.println(line._2)
      })
    })
    written
  }

  /**
   * Splits the input file, sorts each chunk, and writes it to a temp file.
   * @param src The file to sort
   * @param dst The destination file.  Used only if there is only one
   * output temp file (i.e. a small file to be sorted).
   * @return The queue of temp files, in the order they were created.
   */
  private def sort(src: File, dst: File): Queue[File] = {
    var i = 0
    var tmpFiles = Queue[File]()

    // write out individual runs
    // buf is an array of key columns to line read
    val buf: Array[(String, String)] = new Array[(String, String)](sortSz)
    withInput(src, br => {
      var line = br.readLine
      while (null != line) {
        val pieces = line.split(sep)
        val next = (makeKey(pieces), line)
        buf(i) = next
        line = br.readLine

        if ((i + 1) == sortSz) {
          // read ahead so we know whether we only have one temp file
          val dstOpt = if (null == line && tmpFiles.isEmpty) {
            Some(dst)
          } else { None }
          // write i + 1 elements to disk as we have a full buffer and i has
          // not been incremented
          tmpFiles = tmpFiles.enqueue(write(buf, i + 1, dstOpt))
          i = 0
        } else {
          i += 1
        }
      }
    })

    // write remainder
    if (i > 0 && i < sortSz) {
      val dstOpt = if (tmpFiles.isEmpty) { Some(dst) } else { None }
      // i is one ahead of last valid element-don't add 1 as we did earlier
      tmpFiles = tmpFiles.enqueue(write(buf, i, dstOpt))
    }
    tmpFiles
  }

  /**
   * Merges the data in tmpFiles into a fully sorted file.
   * @param tmpFiles The files to merge.  These must already be sorted.
   * @param filenameOrder The Ordering for the priority queue, which compares
   * a tuple of (file name, (key array, file line)).
   * @return The destination file that was written to.
   */
  private def merge(tmpFiles: Queue[File], dstOpt: Option[File]): File = {
    var streams: Map[String, BufferedReader] = LinkedHashMap[String, BufferedReader]()
    val written = fileToWrite(dstOpt)

    // create a map of filename to timestamps for stability during the merge
    var entryCounts: Map[String, Int] = HashMap[String, Int]()
    var tstamp = 0
    // create a function that can generate a new PQStruct based on known input
    // params; evaluate the stable argument only once
    val makeStruct = {
      if (stable) {
        (fname: String, line: String) => {
          val entryCount = entryCounts.getOrElseUpdate(fname, {tstamp += 1; tstamp})
          new PQStruct(fname, entryCount, makeKey(line.split(sep)), line)
        }
      } else {
        (fname: String, line: String) => {
          // we don't care about entryCount, so set it to zero
          new PQStruct(fname, 0, makeKey(line.split(sep)), line)
        }
      }
    }

    try {
      streams = toStreams(tmpFiles)
      var queue = new PriorityQueue[PQStruct]()(filenameOrder)

      // initialize the priority queue
      var deleted: List[String] = Nil
      for ((fname, stream) <- streams) {
        val next = readMaybe(stream)
        if (null == next) {
          deleted ::= fname
        } else {
          queue.enqueue(makeStruct(fname, next))
        }
      }
      // remove empty streams
      if (!deleted.isEmpty) { streams = streams -- deleted }

      withOutput(written, pw => {
        // write the smallest until we're out of streams or the queue is empty
        while (!queue.isEmpty) {
          // pull the smallest off
          val min = queue.dequeue
          if (!streams.isEmpty) {
            // and read from that stream again
            val fname = min.filename
            if (streams.contains(fname)) {
              val stream = streams(fname)
              val next = readMaybe(stream)
              if (null == next) { streams -= fname }
              else {
                queue.enqueue(makeStruct(fname, next))
              }
            }
          }
          // now write the smallest
          pw.println(min.line)
        }

        if (!streams.isEmpty) {
          // this should never happen, but functions as an assert
          throw new RuntimeException("Stream list not empty; contains " + streams.keys)
        }
      })
      written
    } finally {
      streams.values.foreach(safeClose(_))
    }
  }

  /**
   * Runs the merge sort.
   * @param src The input file.
   * @param dst The output file.
   */
  def apply(src: File, dst: File): File = {
    var tmpFiles: Queue[File] = sort(src, dst)

    if (1 == tmpFiles.size) {
      // short circuit and return
      tmpFiles.front
    } else {
      // we update tmpFiles as we go through the various merges, stopping
      // only when there is only one file left in the list
      while (tmpFiles.size > 1) {
        // split the file lists for merge passes
        var split: Queue[Queue[File]] = Queue[Queue[File]]()
        while (!tmpFiles.isEmpty) {
          val (first, second) = tmpFiles.splitAt(mergeSz)
          tmpFiles = second
          if (!first.isEmpty) { split = split.enqueue(first) }
        }
        // reset so we can assign the next pass
        tmpFiles = Queue[File]()

        // handle the case where we are about to write to the final file
        val dstOpt = if (1 == split.size) { Some(dst) } else { None }
        for (fileList <- split) {
          tmpFiles = tmpFiles.enqueue(merge(fileList, dstOpt))
        }
      }
      tmpFiles.front
    }
  }

  // Utility methods
  /**
   * Returns a temp file or the contained File in dstOpt, depending on whether
   * dstOpt is None or not, respectively.
   */
  private def fileToWrite(dstOpt: Option[File]): File = {
    dstOpt.getOrElse({
      val tmp = File.createTempFile("cb-merge", ".dat")
      tmp.deleteOnExit
      tmp
    })
  }

  /**
   * Concatenates the elements in pieces whose index matches those in cols.
   */
  private def makeKey(pieces: Array[String]) = {
    val sb = new StringBuilder()
    for (col <- cols) {
      sb.append(pieces(col))
    }
    sb.toString
  }
  // end Utility methods
}

object MergeSort {
  def main(optArgs: Array[String]): Unit = {
    val sortSzDefault = 10000
    val mergeSzDefault = 32
    var args: List[String] = Nil
    var sortSz: Int = sortSzDefault
    var mergeSz: Int = mergeSzDefault
    var cols = new ArrayBuffer[Int]()
    var sep = " "
    var reverse = false
    var stable = false
    var help = false
    for (optArg <- optArgs) {
      if (optArg.startsWith("--field-separator=")) {
        sep = optArg.replaceFirst("--field-separator=", "")
      } else if (optArg.startsWith("--key=")) {
        val keys = optArg.replaceFirst("--key=", "").split(",").map(_.toInt)
        if (keys.length < 2) {
          cols = cols ++ Range(keys(0) - 1, keys(0))
        } else {
          cols = cols ++ Range(keys(0) - 1, keys(1))
        }
      } else if (optArg.startsWith("--sort-size=")) {
        sortSz = optArg.replaceFirst("--sort-size=", "").toInt
      } else if (optArg.startsWith("--merge-size=")) {
        mergeSz = optArg.replaceFirst("--merge-size=", "").toInt
      } else if (optArg.startsWith("--stable")) {
        stable = true
      } else if (optArg.equals("--reverse") || optArg.equals("-r")) {
        reverse = true
      } else if (optArg.equals("--help") || optArg.equals("-h")) {
        help = true
      } else if (optArg.startsWith("-")) {
        Console.println("Unknown option " + optArg)
        System.exit(1)
      } else {
        args ::= optArg
      }
    }
    args = args.reverse
    if (help || args.length < 2) {
      Console.println("""Usage: mergesort [options] <src> <dst>
Options (must be followed with an equals sign):
--field-separator  The file field separator.  Default: , (comma)
--key A comma separated list of key columns, starting at 1.  Default: 1 (sort on first column)
--sort-size The maximum number of records to sort in-memory at once.  Default: %s
--merge-size The maximum number of files to merge at once.  Default: %s
--reverse Reverse the sort order.
--help Show this usage statement""".format(sortSzDefault, mergeSzDefault))
      System.exit(1)
    }

    val src = new File(args(0))
    val dst = new File(args(1))

    if (src.getAbsolutePath == dst.getAbsolutePath) {
      throw new IllegalArgumentException("Source and destination cannot be the same file.")
    }

    if (cols.isEmpty) {
      cols += 0
    }

    new MergeSort(cols.toArray, sortSz, mergeSz, sep, reverse, stable)(src, dst)
  }
}
