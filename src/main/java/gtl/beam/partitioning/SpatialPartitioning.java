package gtl.beam.partitioning;

import gtl.exception.UnimplementedException;
import gtl.geom.Envelope;
import gtl.geom.Vector;
import gtl.util.ArrayUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class  SpatialPartitioning< E extends Envelope> implements Serializable{

    private static final long serialVersionUID=1L;

    public static final int EQUAL_PARTITIONING=1;
    public static final int HILBERT_PARTITIONING=2;
    public static final int KDTREE_PARTITIONING=3;
    public static final int QUADTREE_PARTITIONING=4;
    public static final int RTREE_PARTITIONING=5;
    public static final int STRTREE_PARTITIONING=6;
    public static final int VORONOI_PARTITIONING=7;


    protected Collection<Envelope> partitionEnvelopes;
    protected Envelope totalExtent;
    SpatialPartitioning(E totalExtent ){
        if(totalExtent!=null)
            this.totalExtent=(Envelope)totalExtent.clone();
        else
            this.totalExtent=Envelope.create(2);
        this.partitionEnvelopes=new ArrayList<>();
    }

    public Collection<Envelope> getPartitionEnvelopes() {
        return this.partitionEnvelopes;
    }

    public Envelope getTotalExtent() {
        return this.totalExtent;
    }

    public int getPartitionCount() {
        if(this.partitionEnvelopes==null)
            return 0;
        else
            return this.partitionEnvelopes.size();
    }

    public static <E extends Envelope > QuadTreePartitioning<E> createQuadTreePartitioning(E  totalExtent, Iterable<E> samples, int partitions){
        return new QuadTreePartitioning(totalExtent,samples,partitions);
    }

    public static <E extends Envelope > RTreePartitioning<E> createRTreePartitioning(E  totalExtent, Iterable<E> samples, int partitions){
        return new RTreePartitioning(totalExtent,samples,partitions);
    }

    public static  <E extends Envelope > STRTreePartitioning<E> createSTRTreePartitioning(E  totalExtent, Iterable<E> samples, int partitions){
        return new STRTreePartitioning(totalExtent,samples,partitions);
    }

    public static <E extends Envelope > EqualPartitioning<E> createEqualPartitioning(E  totalExtent,  int partitions){
        return new EqualPartitioning(totalExtent,partitions);
    }

    public  static <E extends Envelope > HilbertPartitioning<E> createHilbertPartitioning(E  totalExtent, Iterable<E> samples, int partitions){
        try {
            return new HilbertPartitioning(totalExtent,samples,partitions);
        }
        catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    public static <E extends Envelope, V extends Vector> KDTreePartitioning<E,V> createKDTreePartitioning(E totalExtent, Iterable<V> samples ){
        return new KDTreePartitioning(totalExtent,samples);
    }

    public static <E extends Envelope > VoronoiPartitioning<E> createVoronoiPartitioning(E  totalExtent, Iterable<E> samples, int partitions){
        return new VoronoiPartitioning(totalExtent,samples,partitions);
    }

    public static <E extends Envelope > SpatialPartitioning<E> createPartitioning(int type , E  totalExtent, List<E> samples, int partitions){
        switch (type){
            case EQUAL_PARTITIONING: {
                return createEqualPartitioning(totalExtent, partitions);
            }
            case HILBERT_PARTITIONING: {
                return createHilbertPartitioning(totalExtent, samples, partitions);
            }
            case KDTREE_PARTITIONING:{
                ArrayList<Vector> alv = new ArrayList<>();
                for(E e: samples)
                    alv.add(e.getCenter());
                return createKDTreePartitioning(totalExtent,alv);
            }
            case QUADTREE_PARTITIONING:{
                return createQuadTreePartitioning(totalExtent,samples,partitions);
            }
            case RTREE_PARTITIONING:{
                return createRTreePartitioning(totalExtent,samples,partitions);
            }
            case STRTREE_PARTITIONING:{
                return createSTRTreePartitioning(totalExtent,samples,partitions);
            }
            case VORONOI_PARTITIONING:{
                return createVoronoiPartitioning(totalExtent,samples,partitions);
            }
            default:{
                try {
                    throw new UnimplementedException("public static <E extends Envelope > SpatialPartitioning<E> createPartitioning(int type , E  totalExtent, List<E> samples, int partitions)");
                }
                catch (Exception e){
                    e.printStackTrace();
                }
                return null;
            }
        }
    }

    public static <E extends Envelope > SpatialPartitioning<E> createPartitioning(int type , E  totalExtent, Iterable<E> samples, int partitions){
         return createPartitioning(type,totalExtent, ArrayUtils.iterableToList(samples),partitions);
    }
}
