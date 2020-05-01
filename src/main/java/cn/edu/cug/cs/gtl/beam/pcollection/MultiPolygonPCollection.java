package cn.edu.cug.cs.gtl.beam.pcollection;

import cn.edu.cug.cs.gtl.beam.pipeline.SpatialPipeline;
import cn.edu.cug.cs.gtl.geom.Geometry;
import cn.edu.cug.cs.gtl.geom.MultiPolygon;
import cn.edu.cug.cs.gtl.io.FileDataSplitter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;

import java.io.Serializable;
import java.util.List;

public class MultiPolygonPCollection extends GeometryPCollection<MultiPolygon> implements Serializable {
    private static final long serialVersionUID = 1L;

    public MultiPolygonPCollection(Preprocesser pp, int partitions) {
        super(pp, partitions);
    }

    public MultiPolygonPCollection(Pipeline p, List<MultiPolygon> geometries) {
        super(p, geometries);
    }

    public MultiPolygonPCollection(SpatialPipeline p) {
        super(p);
    }

    public MultiPolygonPCollection(Pipeline p) {
        super(p);
    }

    public MultiPolygonPCollection(SpatialPipeline p, List<MultiPolygon> geometries) {
        super(p, geometries);
    }

    public MultiPolygonPCollection(SpatialPipeline p, String inputLocation, FileDataSplitter splitter) {
        super(p, inputLocation, splitter);
    }

    public MultiPolygonPCollection(Pipeline p, String inputLocation, FileDataSplitter splitter) {
        super(p, inputLocation, splitter);
    }

    public MultiPolygonPCollection(PCollection<MultiPolygon> p) {
        super(p);
    }

    public boolean loadFromCSVFile(String fileName){
        return super.loadFromCSVFile(fileName, Geometry.MULTIPOLYGON);
    }

    public boolean loadFromTSVFile(String fileName){
        return super.loadFromTSVFile(fileName,Geometry.MULTIPOLYGON);
    }

}
