package gtl.beam.pcollection;

import gtl.beam.pipeline.SpatialPipeline;
import gtl.geom.Geometry;
import gtl.geom.MultiPoint;
import gtl.io.FileDataSplitter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;
import java.util.List;

public class MultiPointPCollection extends GeometryPCollection<MultiPoint> implements Serializable {
    private static final long serialVersionUID = 1L;

    public MultiPointPCollection(Preprocesser pp, int partitions) {
        super(pp, partitions);
    }

    public MultiPointPCollection(Pipeline p, List<MultiPoint> geometries) {
        super(p, geometries);
    }

    public MultiPointPCollection(SpatialPipeline p) {
        super(p);
    }

    public MultiPointPCollection(Pipeline p) {
        super(p);
    }

    public MultiPointPCollection(SpatialPipeline p, List<MultiPoint> geometries) {
        super(p, geometries);
    }

    public MultiPointPCollection(SpatialPipeline p, String inputLocation, FileDataSplitter splitter) {
        super(p, inputLocation, splitter);
    }

    public MultiPointPCollection(Pipeline p, String inputLocation, FileDataSplitter splitter) {
        super(p, inputLocation, splitter);
    }

    public MultiPointPCollection(PCollection<MultiPoint> p) {
        super(p);
    }

    public boolean loadFromCSVFile(String fileName){
        return super.loadFromCSVFile(fileName, Geometry.MULTIPOINT);
    }

    public boolean loadFromTSVFile(String fileName){
        return super.loadFromTSVFile(fileName,Geometry.MULTIPOINT);
    }

}
