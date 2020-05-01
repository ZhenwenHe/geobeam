package cn.edu.cug.cs.gtl.beam.pcollection;

import cn.edu.cug.cs.gtl.beam.pipeline.SpatialPipeline;
import cn.edu.cug.cs.gtl.geom.Geometry;
import cn.edu.cug.cs.gtl.geom.MultiPolygon;
import cn.edu.cug.cs.gtl.io.FileDataSplitter;

import java.io.Serializable;

public class IndexableMultiPolygonPCollection extends IndexablePCollection<MultiPolygon> implements Serializable {
    private static final long serialVersionUID = 1L;

    public IndexableMultiPolygonPCollection(Preprocesser pp, int partitions, int indexType, int indexCapacity, int leafCapacity) {
        super(pp, partitions, indexType, indexCapacity, leafCapacity);
    }

    public IndexableMultiPolygonPCollection(SpatialPipeline p, String inputLocation, FileDataSplitter splitter, int indexType, int indexCapacity, int leafCapacity) {
        super(p, inputLocation, splitter, indexType, indexCapacity, leafCapacity);
    }
    public boolean loadFromCSVFile(String fileName){
        return super.loadFromCSVFile(fileName, Geometry.MULTIPOLYGON);
    }

    public boolean loadFromTSVFile(String fileName){
        return super.loadFromTSVFile(fileName,Geometry.MULTIPOLYGON);
    }

}
