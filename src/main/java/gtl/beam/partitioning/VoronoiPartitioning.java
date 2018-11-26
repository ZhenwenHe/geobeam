package gtl.beam.partitioning;

import gtl.geom.Envelope;
import gtl.jts.geom.Geom2DSuits;
import gtl.util.ArrayUtils;

import java.io.Serializable;
import java.util.List;

public class VoronoiPartitioning <E extends Envelope >
        extends SpatialPartitioning<E> implements Serializable {

    private static final long serialVersionUID=1L;

    public VoronoiPartitioning(E totalExtent,List<E> samples, int partitions) {
        super(totalExtent);
        this.partitionEnvelopes.clear();
        this.partitionEnvelopes.addAll(
                Geom2DSuits.createVoronoiPartitioning((List<Envelope>) samples,partitions));
        for(Envelope e: partitionEnvelopes)
            this.totalExtent.combine(e);
    }
    public VoronoiPartitioning(E totalExtent,Iterable<E> samples, int partitions) {
        this(totalExtent, ArrayUtils.iterableToList(samples),partitions);
    }
}
