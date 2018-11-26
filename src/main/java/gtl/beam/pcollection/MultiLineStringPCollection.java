package gtl.beam.pcollection;

import gtl.beam.pipeline.SpatialPipeline;
import gtl.geom.Geometry;
import gtl.geom.MultiLineString;
import gtl.io.FileDataSplitter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;
import java.util.List;

public class MultiLineStringPCollection
        extends GeometryPCollection<MultiLineString> implements Serializable {
    private static final long serialVersionUID = 1L;

    public MultiLineStringPCollection(Preprocesser pp, int partitions) {
        super(pp, partitions);
    }

    public MultiLineStringPCollection(Pipeline p, List<MultiLineString> geometries) {
        super(p, geometries);
    }

    public MultiLineStringPCollection(SpatialPipeline p) {
        super(p);
    }

    public MultiLineStringPCollection(Pipeline p) {
        super(p);
    }

    public MultiLineStringPCollection(SpatialPipeline p, List<MultiLineString> geometries) {
        super(p, geometries);
    }

    public MultiLineStringPCollection(SpatialPipeline p, String inputLocation, FileDataSplitter splitter) {
        super(p, inputLocation, splitter);
    }

    public MultiLineStringPCollection(Pipeline p, String inputLocation, FileDataSplitter splitter) {
        super(p, inputLocation, splitter);
    }

    public MultiLineStringPCollection(PCollection<MultiLineString> p) {
        super(p);
    }

    public boolean loadFromCSVFile(String fileName){
        return super.loadFromCSVFile(fileName, Geometry.MULTILINESTRING);
    }

    public boolean loadFromTSVFile(String fileName){
        return super.loadFromTSVFile(fileName,Geometry.MULTILINESTRING);
    }

}
