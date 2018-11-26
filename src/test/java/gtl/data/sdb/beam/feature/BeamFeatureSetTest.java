package gtl.data.sdb.beam.feature;

import gtl.geom.Geometry;
import gtl.io.FileDataSplitter;
import org.junit.Test;

import static org.junit.Assert.*;

public class BeamFeatureSetTest {

    @Test
    public void readTSVSchema() {
        String input ="WKT%POLYGON\tSTATEFP\tCOUNTYFP\tCOUNTYNS\tGEOID\tNAME\tNAMELSAD\tLSAD\tCLASSFP\tMTFCC\tCSAFP\tCBSAFP\tMETDIVFP\tFUNCSTAT\tALAND\tAWATER\tINTPTLAT\tINTPTLON";
        String[] columns = input.split(FileDataSplitter.TSV.getDelimiter());
        String []geomInfo = columns[0].split(FileDataSplitter.PERCENT.getDelimiter());
        assert geomInfo.length==2;
        String p = Geometry.getTypeBinding(Geometry.getType(geomInfo[1])).getSimpleName();
        System.out.println(p);
    }
}