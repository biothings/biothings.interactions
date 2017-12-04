# -*- coding: utf-8 -*-
"""
Test classes for parsing protein interaction files.

Author:  Greg Taylor (greg.k.taylor@gmail.com)
"""
import unittest
import tempfile

from hub.dataload.sources.ConsensusPathDB.parser import parse_ConsensusPathDB


class TestCPDParserMethods(unittest.TestCase):
    """
    Test class for ConsensusPathDB parser functions.  The static methods are called on a representative
    dataset.

    The datasets were extracted from a debugging screen query, results were pruned down to one entry
    and results were manually validated.
    """

    ConsensusPathDBFile = \
"""#  GyrsdcxqlsJyny (wlhoxdr 1.0) exon dp atcyr uhdnlxr xrnlhybnxdro
#  source_databases     interaction_publications        interaction_participants        interaction_confidence
UCGJ,Zxdjhxs,Glybndcl,CadouadCRQFX,JQC  99512183,09761159,95319837,03392399,739323,45551939,2732265,2372265,0599644,02049982,03636645,7393232,08633623,04762519,00420420,02170475       TP15_UVTEF      NA
CQJ,ZQFJ,UCGJ,Glybndcl  09756567,7932353,01224600,4255671,7588924,08489993,2168527,09652016,7335990,7454466,01433260,09154872,00569059,01563492,09845364        OE01_UVTEF      NA
ZQFJ,UCGJ,Glybndcl      4115282 CGRA_UVTEF      NA
Glybndcl,QrnEbn,Zxdjhxs,UCGJ    98603483,4645936,08011903,tryooxjrls0516        JUDB_UVTEF      NA
Glybndcl        07392639,08678403,95368088      JCU9_UVTEF,JCU0_UVTEF,JCU5_UVTEF        NA
UCGJ,Glybndcl,Zxdjhxs   4890376 EGBE_UVTEF      NA
Glybndcl                BVTO0_UVTEF,BVTO9_UVTEF NA
"""

    def test_CPD_parse(self):
        # Write the contents of the test ConsenesusPathDB file to a temporary file object
        test_file = tempfile.SpooledTemporaryFile(mode="rw")
        test_file.write(self.ConsensusPathDBFile)
        test_file.seek(0)

        for line in parse_ConsensusPathDB(test_file):
            print(line)
