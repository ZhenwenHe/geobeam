package gtl.beam.partitioning;

import gtl.common.Identifier;
import gtl.geom.Envelope;
import gtl.index.rtree.RTree;
import gtl.index.shape.RegionShape;
import gtl.util.ArrayUtils;

import java.io.Serializable;
import java.util.List;

public class RTreePartitioning  <E extends Envelope>
        extends SpatialPartitioning<E> implements Serializable {

    private static final long serialVersionUID=1L;
    public RTreePartitioning(E totalExtent, List<E> samples, int partitions) {
        super(totalExtent);
        RTree rTree =RTree.create(2,32,samples.size()/partitions);
        long i=0;
        for(E e: samples){
            rTree.insert(null,new RegionShape(e), Identifier.create(i));
            ++i;
        }
        this.partitionEnvelopes.clear();
        this.partitionEnvelopes.addAll(rTree.getPartitionEnvelopes());
        for(Envelope e: partitionEnvelopes)
            this.totalExtent.combine(e);
    }
    public RTreePartitioning(E totalExtent, Iterable<E> samples, int partitions){
        this(totalExtent, ArrayUtils.iterableToList(samples),partitions);
    }
}
