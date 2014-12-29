#!/usr/bin/env python
# Copyright 2010 Krysta M Bouzek
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the version 3 of the GNU Lesser General Public License
# as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
from __future__ import with_statement
from optparse import OptionParser
import random
import sys
from datetime import datetime,date,time

def main(argv=None):
  if argv is None:
    argv = sys.argv[1:]

  usage = "%prog dictionary_file"
  parser = OptionParser()
  parser.add_option("-t", action="store", dest="delim", default=",", help="delimiter")
  parser.add_option("--use-dates", action="store_true", dest="useDates", default=False, help="Use a random date for the first column.")

  (opts, args) = parser.parse_args(argv)
  if len(args) < 1:
    dictionary = "/usr/share/dict/words"
  else:
    dictionary = args[0]

  words = []
  delim = opts.delim
  with open(dictionary) as fh:
    for line in fh:
      if line.find("'") < 0:
        words.append(line.strip())

  if opts.useDates:
    yyyy = [y for y in range(1990,2011)]
    mm = [m for m in range(1,13)]
    dd = [d for d in range(1,29)]
    hh = [h for h in range(0,25)]
    mn = [m for m in range(0,60)]

    curMo = random.choice(mm)
    curYr = random.choice(yyyy)
    for i in range(0,1000):
      curHr = random.choice(hh)
      d = "%02d-%02d-%02d %02d:%02d" % (curYr, curMo, random.choice(dd), curHr, random.choice(mm))
      if 0 == (i % 15):
        curYr = random.choice(yyyy)
      elif 0 == (i % 5):
        curMo = random.choice(mm)
      print "%s%s%s" % (d, delim, delim.join(map(lambda w: random.choice(words), range(3))))
  else:
    for i in range(0,1000):
      print delim.join(map(lambda w: random.choice(words), range(3)))

if __name__ == '__main__':
  sys.exit(main())
