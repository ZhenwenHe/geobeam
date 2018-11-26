package gtl.beam.pcollection;

import gtl.beam.pipeline.SpatialPipeline;
import gtl.geom.Geometry;
import gtl.geom.Point;
import gtl.io.FileDataSplitter;

import java.io.Serializable;

public class IndexablePointPCollection extends IndexablePCollection<Point> implements Serializable {
    private static final long serialVersionUID = 1L;

    public IndexablePointPCollection(Preprocesser pp, int partitions, int indexType, int indexCapacity, int leafCapacity) {
        super(pp, partitions, indexType, indexCapacity, leafCapacity);
    }

    public IndexablePointPCollection(SpatialPipeline p, String inputLocation, FileDataSplitter splitter, int indexType, int indexCapacity, int leafCapacity) {
        super(p, inputLocation, splitter, indexType, indexCapacity, leafCapacity);
    }

    public boolean loadFromCSVFile(String fileName){
        return super.loadFromCSVFile(fileName, Geometry.POINT);
    }

    public boolean loadFromTSVFile(String fileName){
        return super.loadFromTSVFile(fileName,Geometry.POINT);
    }

}
