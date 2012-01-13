#!/usr/bin/env python

import sys
import os

if len(sys.argv) != 3:
    print """
This script corrects the two problems with SIPHT workflows: 
    1) It sets the runtime to 33.00 for Blast_synteny, and
    2) it removes all Last_transfer jobs

It works on both .dax and .dag files
"""
    print "Usage: python %s OLDFILE NEWFILE" % sys.argv[0]
    exit(1)

oldfile = os.path.abspath(sys.argv[1])
newfile = os.path.abspath(sys.argv[2])

if not os.path.isfile(oldfile):
    print "%s does not exist" % olddir

print oldfile
print newfile

dax = oldfile.endswith(".dax")

infile = open(oldfile, "r")
outfile = open(newfile, "w")

inltjob = False
inltchild = False
ltids = []
for l in infile:
    if dax:
        if "Blast_synteny" in l:
            l = l.replace('runtime="0.00"','runtime="33.00"')
        elif "Last_transfer" in l:
            inltjob = True
            s = l.index('"') + 1
            e = l.index('"', s)
            ltids.append(l[s:e])
        
        for ltid in ltids:
            if '<child ref="%s"' % ltid in l:
                inltchild = True
        
        if not inltjob and not inltchild:
            outfile.write(l)
        
        if inltjob and "</job>" in l:
            inltjob = False
        
        if inltchild and "</child>" in l:
            inltchild = False
    else:
        if "Blast_synteny" in l:
            rec = l.split()
            rec[-1] = "33.00\n"
            l = " ".join(rec)
        elif "Last_transfer" in l:
            rec = l.split()
            ltids.append(rec[1])
            l = None
        else:
            for ltid in ltids:
                if ltid in l:
                    l = None
                    break
        
        if l is not None:
            outfile.write(l)

infile.close()
outfile.close()
