package gtl.beam.pcollection;

import gtl.beam.pipeline.SpatialPipeline;
import gtl.geom.Geometry;
import gtl.geom.Point;
import gtl.io.FileDataSplitter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;
import java.util.List;

public class PointPCollection extends GeometryPCollection<Point> implements Serializable {
    private static final long serialVersionUID = 1L;

    public PointPCollection(Preprocesser pp, int partitions) {
        super(pp, partitions);
    }

    public PointPCollection(Pipeline p, List<Point> geometries) {
        super(p, geometries);
    }

    public PointPCollection(SpatialPipeline p, List<Point> geometries) {
        super(p, geometries);
    }

    public PointPCollection(SpatialPipeline p, String inputLocation, FileDataSplitter splitter) {
        super(p, inputLocation, splitter);
    }

    public PointPCollection(Pipeline p, String inputLocation, FileDataSplitter splitter) {
        super(p, inputLocation, splitter);
    }

    public PointPCollection(Pipeline p) {
        super(p);
    }

    public PointPCollection(SpatialPipeline p) {
        super(p);
    }

    public PointPCollection(PCollection<Point> p) {
        super(p);
    }

    public boolean loadFromCSVFile(String fileName){
        return super.loadFromCSVFile(fileName, Geometry.POINT);
    }

    public boolean loadFromTSVFile(String fileName){
        return super.loadFromTSVFile(fileName,Geometry.POINT);
    }

}
