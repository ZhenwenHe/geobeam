package cn.edu.cug.cs.gtl.beam.partitioning;

import cn.edu.cug.cs.gtl.common.Identifier;
import cn.edu.cug.cs.gtl.geom.Envelope;
import cn.edu.cug.cs.gtl.index.quadtree.QuadTree;
import cn.edu.cug.cs.gtl.index.shape.RegionShape;
import cn.edu.cug.cs.gtl.util.ArrayUtils;

import java.io.Serializable;
import java.util.List;

public class QuadTreePartitioning  <E extends Envelope>
        extends SpatialPartitioning<E> implements Serializable {

    private static final long serialVersionUID=1L;
    public QuadTreePartitioning(E totalExtent, List<E> samples, int partitions) {
        super(totalExtent);
        QuadTree quadTree =QuadTree.create(samples.size()/partitions,
                totalExtent.getDimension(),
                totalExtent);
        long i=0;
        for(E e: samples){
            quadTree.insert(null,new RegionShape(e), Identifier.create(i));
            ++i;
        }
        this.partitionEnvelopes.clear();
        this.partitionEnvelopes.addAll(quadTree.getPartitionEnvelopes());
        for(Envelope e: partitionEnvelopes)
            this.totalExtent.combine(e);
    }

    public QuadTreePartitioning(E totalExtent, Iterable<E> samples, int partitions){
        this(totalExtent, ArrayUtils.<E>iterableToList(samples),partitions);
    }
}
