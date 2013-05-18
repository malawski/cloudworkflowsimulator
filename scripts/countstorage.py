#!/usr/bin/python

import sys

def main():
    filename = sys.argv[1]

    filemap = {}
    transfer = 0;
    size = 0;

    for line in open(filename, 'r'):
        splitted = line.split()

        if splitted[0] == 'FILE':
            filemap[splitted[1]] = int(splitted[2])
            size += int(splitted[2])

        if splitted[0] == 'OUTPUTS' or splitted[0] == 'INPUTS':
            for fname in splitted[2:]:
                 transfer += filemap[fname]

    mainFileName = filename.split('/')[-1]
    print("filename: {0},\t transfer sum GBs: {1:.2f},\t filesize sum GBs: {2:.2f}".format(mainFileName, float(transfer) / (1024**3), float(size) / (1024**3)))



if __name__ == "__main__":
    main()