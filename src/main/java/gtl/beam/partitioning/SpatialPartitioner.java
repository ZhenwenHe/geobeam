package gtl.beam.partitioning;

import gtl.geom.Envelope;
import gtl.geom.Geometry;
import gtl.util.ArrayUtils;
import org.apache.beam.sdk.transforms.Partition;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

public class SpatialPartitioner<E extends Envelope, G extends Geometry> implements Partition.PartitionFn<G>,Serializable{

    private final static long serialVersionUID = 1L;

    Collection<Envelope> partitionEnvelopes=null;


    public SpatialPartitioner(Collection<E> partitionEnvelopes) {
        if(this.partitionEnvelopes==null)
            this.partitionEnvelopes= new ArrayList<Envelope>();

        this.partitionEnvelopes.clear();
        this.partitionEnvelopes.addAll(partitionEnvelopes);
    }

    public SpatialPartitioner(Iterable<E> partitionEnvelopes) {
        this(ArrayUtils.iterableToCollection(partitionEnvelopes));
    }

    public Collection<Envelope> getPartitionEnvelopes() {
        return partitionEnvelopes;
    }

    public void setPartitionEnvelopes(Collection<? extends E> partitionEnvelopes) {
        this.partitionEnvelopes.clear();
        this.partitionEnvelopes.addAll(partitionEnvelopes);
    }

    @Override
    public int partitionFor(G elem, int numPartitions) {
        int i=0;
        for(Envelope e: partitionEnvelopes){
            if(e.contains(elem.getEnvelope().getCenter()))
                return i;
            ++i;
        }
        return 0;
    }

    public static  <E extends Envelope, G extends Geometry> Partition<G> createPartitionTransform(Iterable<E> es){
        SpatialPartitioner sp = new SpatialPartitioner<E,G>(es);
        return Partition.<G>of(sp.partitionEnvelopes.size(),sp);
    }

    public static  <E extends Envelope, G extends Geometry> Partition<G> createPartitionTransform(Collection<E> es){
        SpatialPartitioner sp = new SpatialPartitioner<E,G>(es);
        return Partition.<G>of(sp.partitionEnvelopes.size(),sp);
    }
}
