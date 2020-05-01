package cn.edu.cug.cs.gtl.beam.pcollection;

import cn.edu.cug.cs.gtl.beam.pipeline.SpatialPipeline;
import cn.edu.cug.cs.gtl.geom.Geometry;
import cn.edu.cug.cs.gtl.geom.LinearRing;
import cn.edu.cug.cs.gtl.io.FileDataSplitter;

import java.io.Serializable;

public class IndexableLinearRingPCollection extends IndexablePCollection<LinearRing>  implements Serializable {
    private static final long serialVersionUID = 1L;

    public IndexableLinearRingPCollection(Preprocesser pp, int partitions, int indexType, int indexCapacity, int leafCapacity) {
        super(pp, partitions, indexType, indexCapacity, leafCapacity);
    }

    public IndexableLinearRingPCollection(SpatialPipeline p, String inputLocation, FileDataSplitter splitter, int indexType, int indexCapacity, int leafCapacity) {
        super(p, inputLocation, splitter, indexType, indexCapacity, leafCapacity);
    }

    public boolean loadFromCSVFile(String fileName){
        return super.loadFromCSVFile(fileName, Geometry.LINEARRING);
    }

    public boolean loadFromTSVFile(String fileName){
        return super.loadFromTSVFile(fileName,Geometry.LINEARRING);
    }

}
