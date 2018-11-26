package gtl.beam.pcollection;

import gtl.beam.pipeline.SpatialPipeline;
import gtl.geom.Geometry;
import gtl.geom.Polygon;
import gtl.io.FileDataSplitter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;
import java.util.List;

public class PolygonPCollection extends GeometryPCollection<Polygon> implements Serializable {
    private static final long serialVersionUID = 1L;

    public PolygonPCollection(Preprocesser pp, int partitions) {
        super(pp, partitions);
    }

    public PolygonPCollection(Pipeline p, List<Polygon> geometries) {
        super(p, geometries);
    }

    public PolygonPCollection(SpatialPipeline p) {
        super(p);
    }

    public PolygonPCollection(Pipeline p) {
        super(p);
    }

    public PolygonPCollection(SpatialPipeline p, List<Polygon> geometries) {
        super(p, geometries);
    }

    public PolygonPCollection(SpatialPipeline p, String inputLocation, FileDataSplitter splitter) {
        super(p, inputLocation, splitter);
    }

    public PolygonPCollection(Pipeline p, String inputLocation, FileDataSplitter splitter) {
        super(p, inputLocation, splitter);
    }

    public PolygonPCollection(PCollection<Polygon> p) {
        super(p);
    }

    public boolean loadFromCSVFile(String fileName){
        return super.loadFromCSVFile(fileName, Geometry.POLYGON);
    }

    public boolean loadFromTSVFile(String fileName){
        return super.loadFromTSVFile(fileName,Geometry.POLYGON);
    }

}
