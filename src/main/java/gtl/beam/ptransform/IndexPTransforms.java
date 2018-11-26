package gtl.beam.ptransform;

import gtl.beam.fn.GeometriesToIndexCombineFn;
import gtl.geom.Envelope;
import gtl.geom.Geometry;
import gtl.index.Indexable;
import org.apache.beam.sdk.transforms.Combine;

public class IndexPTransforms {

    public static <G extends Geometry, I extends Indexable> Combine.Globally<G,I> createGeometriesToIndexCombiner(
            int indexType, Envelope totalExtent, int indexCapacity, int leafCapacity ){
        return Combine.<G,I>globally(GeometriesToIndexCombineFn.<G,I>create(indexType,  totalExtent,  indexCapacity,  leafCapacity));
    }
}
