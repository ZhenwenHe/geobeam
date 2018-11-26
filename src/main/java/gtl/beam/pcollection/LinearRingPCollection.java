package gtl.beam.pcollection;

import gtl.beam.pipeline.SpatialPipeline;
import gtl.geom.Geometry;
import gtl.geom.LinearRing;
import gtl.io.FileDataSplitter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;
import java.util.List;

public class LinearRingPCollection extends GeometryPCollection<LinearRing> implements Serializable {
    private static final long serialVersionUID = 1L;

    public LinearRingPCollection(Preprocesser pp, int partitions) {
        super(pp, partitions);
    }

    public LinearRingPCollection(Pipeline p, List<LinearRing> geometries) {
        super(p, geometries);
    }

    public LinearRingPCollection(Pipeline p) {
        super(p);
    }

    public LinearRingPCollection(SpatialPipeline p, List<LinearRing> geometries) {
        super(p, geometries);
    }

    public LinearRingPCollection(SpatialPipeline p, String inputLocation, FileDataSplitter splitter) {
        super(p, inputLocation, splitter);
    }

    public LinearRingPCollection(Pipeline p, String inputLocation, FileDataSplitter splitter) {
        super(p, inputLocation, splitter);
    }

    public LinearRingPCollection(SpatialPipeline p) {
        super(p);
    }

    public LinearRingPCollection(PCollection<LinearRing> p) {
        super(p);
    }

    public boolean loadFromCSVFile(String fileName){
        return super.loadFromCSVFile(fileName, Geometry.LINEARRING);
    }

    public boolean loadFromTSVFile(String fileName){
        return super.loadFromTSVFile(fileName,Geometry.LINEARRING);
    }
}
