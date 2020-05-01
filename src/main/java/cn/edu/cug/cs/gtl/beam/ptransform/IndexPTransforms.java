package cn.edu.cug.cs.gtl.beam.ptransform;

import cn.edu.cug.cs.gtl.beam.fn.GeometriesToIndexCombineFn;
import cn.edu.cug.cs.gtl.geom.Envelope;
import cn.edu.cug.cs.gtl.geom.Geometry;
import cn.edu.cug.cs.gtl.index.Indexable;
import org.apache.beam.sdk.transforms.Combine;

public class IndexPTransforms {

    public static <G extends Geometry, I extends Indexable> Combine.Globally<G,I> createGeometriesToIndexCombiner(
            int indexType, Envelope totalExtent, int indexCapacity, int leafCapacity ){
        return Combine.<G,I>globally(GeometriesToIndexCombineFn.<G,I>create(indexType,  totalExtent,  indexCapacity,  leafCapacity));
    }
}
