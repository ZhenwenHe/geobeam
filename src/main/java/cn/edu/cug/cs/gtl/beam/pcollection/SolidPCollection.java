package cn.edu.cug.cs.gtl.beam.pcollection;

import cn.edu.cug.cs.gtl.beam.pipeline.SpatialPipeline;
import cn.edu.cug.cs.gtl.geom.Solid;
import cn.edu.cug.cs.gtl.io.FileDataSplitter;
import cn.edu.cug.cs.gtl.io.Storable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class SolidPCollection extends GeometryPCollection<Solid> implements Storable {

    public SolidPCollection(Preprocesser pp, int partitions) {
        super(pp, partitions);
    }

    public SolidPCollection(SpatialPipeline p, List<Solid> geometries) {
        super(p, geometries);
    }

    public SolidPCollection(SpatialPipeline p, String inputLocation, FileDataSplitter splitter) {
        super(p, inputLocation, splitter);
    }

    public SolidPCollection(Pipeline p, String inputLocation, FileDataSplitter splitter) {
        super(p, inputLocation, splitter);
    }

    public SolidPCollection(Pipeline p, List<Solid> geometries) {
        super(p, geometries);
    }

    public SolidPCollection(SpatialPipeline p) {
        super(p);
    }

    public SolidPCollection(Pipeline p) {
        super(p);
    }

    public SolidPCollection(PCollection<Solid> p) {
        super(p);
    }

    @Override
    public Object clone() {
        return null;
    }

    @Override
    public boolean load(DataInput dataInput) throws IOException {
        return false;
    }

    @Override
    public boolean store(DataOutput dataOutput) throws IOException {
        return false;
    }
}
