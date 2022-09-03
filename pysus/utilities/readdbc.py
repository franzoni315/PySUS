u"""
Created on 16/08/16
by fccoelho
license: GPL V3 or Later
"""
import os
import csv
import gzip
from tqdm import tqdm
from io import BytesIO
from tempfile import NamedTemporaryFile

import pandas as pd
from dbfread import DBF

try:
    from pysus.utilities._readdbc import ffi, lib
except (ImportError, ModuleNotFoundError):
    from _readdbc import ffi, lib


def dbc2dbf(infile, outfile):
    """
    Converts a DATASUS dbc file to a DBF database.
    :param infile: .dbc file name
    :param outfile: name of the .dbf file to be created.
    """
    if isinstance(infile, str):
        infile = infile.encode()
    if isinstance(outfile, str):
        outfile = outfile.encode()
    p = ffi.new("char[]", os.path.abspath(infile))
    q = ffi.new("char[]", os.path.abspath(outfile))

    lib.dbc2dbf([p], [q])

    # print(os.path.exists(outfile))


def dbf_to_csvgz(filename: str, encoding: str='iso-8859-1'):
    """
    Streams a dbf file to gzipped CSV file. The Gzipped csv will be saved on the same path but with a csv.gz extension.
    :param filename: path to the dbf file
    """
    data = DBF(filename, encoding=encoding, raw=False)
    fn = os.path.splitext(filename)[0] + '.csv.gz'

    with gzip.open(fn, 'wt') as gzf:
        for i, d in tqdm(enumerate(data), desc='Converting',):
            if i == 0:
                csvwriter = csv.DictWriter(gzf, fieldnames=d.keys())
                csvwriter.writeheader()
                csvwriter.writerow(d)
            else:
                csvwriter.writerow(d)


