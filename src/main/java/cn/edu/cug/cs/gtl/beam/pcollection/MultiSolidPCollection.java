package cn.edu.cug.cs.gtl.beam.pcollection;

import cn.edu.cug.cs.gtl.beam.pipeline.SpatialPipeline;
import cn.edu.cug.cs.gtl.geom.MultiSolid;
import cn.edu.cug.cs.gtl.io.FileDataSplitter;
import cn.edu.cug.cs.gtl.io.Serializable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class MultiSolidPCollection extends GeometryPCollection<MultiSolid> implements Serializable {

    public MultiSolidPCollection(Preprocesser pp, int partitions) {
        super(pp, partitions);
    }

    public MultiSolidPCollection(SpatialPipeline p, List<MultiSolid> geometries) {
        super(p, geometries);
    }

    public MultiSolidPCollection(SpatialPipeline p, String inputLocation, FileDataSplitter splitter) {
        super(p, inputLocation, splitter);
    }

    public MultiSolidPCollection(Pipeline p, String inputLocation, FileDataSplitter splitter) {
        super(p, inputLocation, splitter);
    }

    public MultiSolidPCollection(Pipeline p, List<MultiSolid> geometries) {
        super(p, geometries);
    }

    public MultiSolidPCollection(SpatialPipeline p) {
        super(p);
    }

    public MultiSolidPCollection(Pipeline p) {
        super(p);
    }

    public MultiSolidPCollection(PCollection<MultiSolid> p) {
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
