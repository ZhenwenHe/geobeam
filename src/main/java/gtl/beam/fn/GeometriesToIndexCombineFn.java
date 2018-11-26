package gtl.beam.fn;

import gtl.geom.Envelope;
import gtl.geom.Geometry;
import gtl.index.Indexable;
import gtl.index.quadtree.QuadTree;
import gtl.index.rtree.RTree;
import gtl.index.strtree.STRTree;
import org.apache.beam.sdk.transforms.Combine;

public class GeometriesToIndexCombineFn<G extends Geometry, I extends Indexable>
        extends Combine.CombineFn<G,GeometriesToIndexCombineFn.Accumulator,I> {
    private static final long serialVersionUID = 1L;
    int indexType;
    Envelope totalExtent;
    int leafCapacity ;
    int indexCapacity;

    public static <G extends Geometry, I extends Indexable> GeometriesToIndexCombineFn<G,I> create(int indexType, Envelope totalExtent, int indexCapacity, int leafCapacity){
        return new GeometriesToIndexCombineFn( indexType,  totalExtent,  indexCapacity,  leafCapacity );
    }

    GeometriesToIndexCombineFn(int indexType, Envelope totalExtent, int indexCapacity, int leafCapacity ){
        this.indexType=indexType;
        this.totalExtent=totalExtent;
        this.leafCapacity=leafCapacity;
        this.indexCapacity=indexCapacity;
    }

    @Override
    public Accumulator createAccumulator() {
        return new Accumulator(indexType,totalExtent,indexCapacity,leafCapacity);
    }

    @Override
    public Accumulator addInput(Accumulator accumulator, G input) {
        if(indexType==0){//STRTree
            STRTree t = (STRTree)  accumulator.indexable;
            t.insert(input.getEnvelope(),input);
        }
        else if(indexType==1) {//RTree
            RTree t = (RTree)  accumulator.indexable;
            t.insert(input);
        }
        else if(indexType==2){//QuadTree
            QuadTree t = (QuadTree)  accumulator.indexable;
            t.insert(input);
        }
        else {
            try {
                throw new IllegalStateException("Accumulator: error index type");
            }
            catch (IllegalStateException e){
                e.printStackTrace();
            }
        }
        return accumulator;
    }

    @Override
    public Accumulator mergeAccumulators(Iterable<Accumulator> accumulators) {
        Accumulator total = accumulators.iterator().next();
        int i=0;
        for(Accumulator a : accumulators){
            if(i==0) continue;
            total.indexable.merge(a.indexable);
            i++;
        }
        return total;
    }

    @Override
    public I extractOutput(Accumulator accumulator) {
        return (I) accumulator.indexable;
    }

    public static class Accumulator implements java.io.Serializable{
        private static final long serialVersionUID = 1L;

        public Indexable indexable;
        public Accumulator(int type, Envelope totalExtent, int indexCapacity, int leafCapacity){
            if(type<0) return;
            if(type==0){//STRTree
                STRTree strTree = STRTree.create(leafCapacity);
                this.indexable=strTree;
            }
            else if(type==1) {//RTree
                RTree rTree = RTree.create(totalExtent.getDimension(),indexCapacity,leafCapacity);
                this.indexable=rTree;
            }
            else if(type==2){//QuadTree
                QuadTree quadTree = QuadTree.create(leafCapacity,totalExtent.getDimension(),totalExtent);
                this.indexable=quadTree;
            }
            else {
                try {
                    throw new IllegalStateException("Accumulator: error index type");
                }
                catch (IllegalStateException e){
                    e.printStackTrace();
                }
            }
        }
    }

}
