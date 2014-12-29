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

import java.io.{File, FileReader, BufferedReader}

import org.scalatest.{ Suite, BeforeAndAfterEach }

/**
 * Tests the MergeSort class.  Assumes it is being run in the "top level", above
 * src
 *
 * @author Krysta Bouzek
 */
class MergeSortSuite extends Suite with BeforeAndAfterEach {
  private val inputDir = new File("src/test/data")
  private val outputDir = new File("out/test/data")

  override def beforeEach = {
    outputDir.mkdirs
  }

  /**
   * Checks that two files are equal, line by line, and that their line
   * counts are the same.
   */
  private def assertFilesEqual(expected: File)(underTest: File): Unit = {
    var testReader: BufferedReader = null
    var expectedReader: BufferedReader = null
    try {
      testReader = new BufferedReader(new FileReader(underTest))
      try {
        expectedReader = new BufferedReader(new FileReader(expected))

        var expectedLine = expectedReader.readLine
        var testLine = testReader.readLine
        while (null != testLine && null != expectedLine) {
          expect(expectedLine, "Mismatch on file %s against %s".format(underTest, expected)) {
            testLine
          }
          expectedLine = expectedReader.readLine
          testLine = testReader.readLine
        }
      } finally {
        if (null != expectedReader) { expectedReader.close }
      }
    } finally {
      if (null != testReader) { testReader.close }
    }
  }

  /**
   * Runs assertFilesEqual on the expected and input files.
   */
  private def assertSort(sorter: MergeSort, expected: String, input: String) = {
    assertFilesEqual(new File(inputDir, expected)) {
      sorter(new File(inputDir, input), new File(outputDir, input))
    }
  }

  def testSimple() = {
    // test a variety of sorting inputs
    val sorters = List(
      new MergeSort(Array[Int](0), 10000, 32, ",", false, true),
      new MergeSort(Array[Int](0), 10, 5, ",", false, true),
      new MergeSort(Array[Int](0), 2, 3, ",", false, true))
    sorters.foreach(sorter => {
      assertSort(sorter, "simpletestExpected.txt", "simpletest.txt")
      assertSort(sorter, "alphanumtestExpected.txt", "alphanumtest.txt")
    })
  }

  def testSortStability() = {
    // first make sure sort-only is stable and correct
    assertSort(new MergeSort(Array[Int](1), 100, 10, ",", false, true), "file_col2_short_exp.csv", "file_col2_short.csv")
    // now test a 3-way split
    assertSort(new MergeSort(Array[Int](1), 2, 2, ",", false, true), "file_col2_short_exp.csv", "file_col2_short.csv")
    assertSort(new MergeSort(Array[Int](1), 2, 3, ",", false, true), "file_col2_short_exp.csv", "file_col2_short.csv")
    // now test an even 2-way split
    assertSort(new MergeSort(Array[Int](1), 3, 3, ",", false, true), "file_col2_short_exp.csv", "file_col2_short.csv")
    assertSort(new MergeSort(Array[Int](1), 4, 3, ",", false, true), "file_col2_short_exp.csv", "file_col2_short.csv")
    assertSort(new MergeSort(Array[Int](1), 5, 3, ",", false, true), "file_col2_short_exp.csv", "file_col2_short.csv")
    assertSort(new MergeSort(Array[Int](1), 3, 2, ",", false, true), "file_col2_short_exp.csv", "file_col2_short.csv")
    assertSort(new MergeSort(Array[Int](1), 4, 2, ",", false, true), "file_col2_short_exp.csv", "file_col2_short.csv")
    assertSort(new MergeSort(Array[Int](1), 5, 2, ",", false, true), "file_col2_short_exp.csv", "file_col2_short.csv")
  }

  def testColSortUnstable() = {
    assertSort(new MergeSort(Array[Int](0), 100, 32, ",", false), "file_col1_date_exp_unstable.csv", "file_col1_date.csv")
    assertSort(new MergeSort(Array[Int](0), 100, 32, ",", false), "file_col1_exp_unstable.csv", "file_col1.csv")
    assertSort(new MergeSort(Array[Int](1,2), 100, 32, ",", false), "file_col23_exp_unstable.csv", "file_col23.csv")
  }

  def testColSort() = {
    assertSort(new MergeSort(Array[Int](0), 100, 32, ",", false, true), "file_col1_date_exp.csv", "file_col1_date.csv")
    assertSort(new MergeSort(Array[Int](0,1,2), 100, 32, "|", false, true), "file_col13_exp.tsv", "file_col13.tsv")
    assertSort(new MergeSort(Array[Int](1), 100, 32, ",", false, true), "file_col2_exp.csv", "file_col2.csv")
    assertSort(new MergeSort(Array[Int](0), 100, 32, ",", false, true), "file_col1_exp.csv", "file_col1.csv")
    assertSort(new MergeSort(Array[Int](1,2), 100, 32, ",", false, true), "file_col23_exp.csv", "file_col23.csv")

    assertSort(new MergeSort(Array[Int](0), 200, 15, ",", false, true), "file_col1_date_exp.csv", "file_col1_date.csv")
    assertSort(new MergeSort(Array[Int](0,1,2), 50, 7, "|", false, true), "file_col13_exp.tsv", "file_col13.tsv")
    assertSort(new MergeSort(Array[Int](1), 200, 15, ",", false, true), "file_col2_exp.csv", "file_col2.csv")
    assertSort(new MergeSort(Array[Int](1,2), 48, 3, ",", false, true), "file_col23_exp.csv", "file_col23.csv")
  }

  def testColSortBoundary() = {
    // the next tests break
    assertSort(new MergeSort(Array[Int](0), 649, 2, ",", false, true), "file_col1_short_exp.csv", "file_col1_short.csv")
    assertSort(new MergeSort(Array[Int](0), 999, 2, ",", false, true), "file_col1_exp.csv", "file_col1.csv")
    assertSort(new MergeSort(Array[Int](0), 999, 3, ",", false, true), "file_col1_exp.csv", "file_col1.csv")
    assertSort(new MergeSort(Array[Int](0), 999, 20, ",", false, true), "file_col1_exp.csv", "file_col1.csv")
  }

  /**
   * Tests a variety of different merge size and sort sizes for stability.
   */
  def testSortPermute() = {
    for (ss <- 5 until 1000 by 51) {
      for (ms <- 2 until 64 by 3) {
        assertSort(new MergeSort(Array[Int](0), ss, ms, ",", false, true), "file_col1_date_exp.csv", "file_col1_date.csv")
        assertSort(new MergeSort(Array[Int](0,1,2), ss, ms, "|", false, true), "file_col13_exp.tsv", "file_col13.tsv")
        assertSort(new MergeSort(Array[Int](1), ss, ms, ",", false, true), "file_col2_exp.csv", "file_col2.csv")
        assertSort(new MergeSort(Array[Int](0), ss, ms, ",", false, true), "file_col1_exp.csv", "file_col1.csv")
        assertSort(new MergeSort(Array[Int](1,2), ss, ms, ",", false, true), "file_col23_exp.csv", "file_col23.csv")
      }
    }
  }

  def testSimpleReversed() = {
    // test a variety of sorting inputs
    val sorters = List(
      new MergeSort(Array[Int](0), 1000, 16, ",", true),
      new MergeSort(Array[Int](0), 9, 6, ",", true),
      new MergeSort(Array[Int](0), 3, 4, ",", true))
    sorters.foreach(sorter => {
      assertFilesEqual(new File(inputDir, "simpletestRevExpected.txt")) {
        sorter(new File(inputDir, "simpletest.txt"), new File(outputDir, "simpletestRev.txt"))
      }

      assertFilesEqual(new File(inputDir, "alphanumtestRevExpected.txt")) {
        sorter(new File(inputDir, "alphanumtest.txt"), new File(outputDir, "alphanumtestRev.txt"))
      }
    })
  }
}
