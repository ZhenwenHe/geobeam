package gtl.beam.pcollection;

import gtl.beam.pipeline.SpatialPipeline;
import gtl.geom.Geometry;
import gtl.geom.LineString;
import gtl.io.FileDataSplitter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;
import java.util.List;

public class LineStringPCollection extends GeometryPCollection<LineString> implements Serializable {
    private static final long serialVersionUID = 1L;

    public LineStringPCollection(Preprocesser pp, int partitions) {
        super(pp, partitions);
    }

    public LineStringPCollection(Pipeline p, List<LineString> geometries) {
        super(p, geometries);
    }

    public LineStringPCollection(Pipeline p) {
        super(p);
    }

    public LineStringPCollection(SpatialPipeline p, List<LineString> geometries) {
        super(p, geometries);
    }

    public LineStringPCollection(SpatialPipeline p, String inputLocation, FileDataSplitter splitter) {
        super(p, inputLocation, splitter);
    }

    public LineStringPCollection(Pipeline p, String inputLocation, FileDataSplitter splitter) {
        super(p, inputLocation, splitter);
    }

    public LineStringPCollection(SpatialPipeline p) {
        super(p);
    }

    public LineStringPCollection(PCollection<LineString> p) {
        super(p);
    }

    public boolean loadFromCSVFile(String fileName){
        return super.loadFromCSVFile(fileName, Geometry.LINESTRING);
    }

    public boolean loadFromTSVFile(String fileName){
        return super.loadFromTSVFile(fileName,Geometry.LINESTRING);
    }
}
