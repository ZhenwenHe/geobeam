package gtl.beam.partitioning;

import gtl.geom.Envelope;
import gtl.index.strtree.impl.STRTreeImpl;
import gtl.util.ArrayUtils;

import java.io.Serializable;
import java.util.List;

public class STRTreePartitioning  <E extends Envelope>
        extends SpatialPartitioning<E> implements Serializable {

    private static final long serialVersionUID=1L;

    public STRTreePartitioning(E totalExtent, List<E> samples, int partitions) {
        super(totalExtent);
        STRTreeImpl strTree=new STRTreeImpl(samples.size()/partitions);
        for(E e: samples){
            strTree.insert((Envelope)e,e);
        }
        this.partitionEnvelopes.clear();
        this.partitionEnvelopes.addAll(strTree.getPartitionEnvelopes());
        for(Envelope e: partitionEnvelopes)
            this.totalExtent.combine(e);

    }

    public STRTreePartitioning(E totalExtent, Iterable<E> samples, int partitions){
        this(totalExtent, ArrayUtils.iterableToList(samples),partitions);
    }
}
