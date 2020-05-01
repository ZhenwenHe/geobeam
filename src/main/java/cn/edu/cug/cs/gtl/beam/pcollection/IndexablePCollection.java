package cn.edu.cug.cs.gtl.beam.pcollection;

import cn.edu.cug.cs.gtl.beam.pipeline.SpatialPipeline;
import cn.edu.cug.cs.gtl.beam.ptransform.IndexPTransforms;
import cn.edu.cug.cs.gtl.beam.ptransform.IterablePTransforms;
import cn.edu.cug.cs.gtl.common.Identifier;
import cn.edu.cug.cs.gtl.geom.Envelope;
import cn.edu.cug.cs.gtl.geom.Geometry;
import cn.edu.cug.cs.gtl.index.Indexable;
import cn.edu.cug.cs.gtl.index.Visitor;
import cn.edu.cug.cs.gtl.index.quadtree.QuadTree;
import cn.edu.cug.cs.gtl.index.rtree.RTree;
import cn.edu.cug.cs.gtl.index.shape.RegionShape;
import cn.edu.cug.cs.gtl.index.strtree.STRTree;
import cn.edu.cug.cs.gtl.io.FileDataSplitter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 1.读入数据
 * 2.计算最佳样本数，进行数据采样
 * 3.根据采样数据，构建SpatialPartitioning
 * 4.获取分区矩形列表
 * 5.对集合数据进行分区
 * 6.对每个分区构建分区索引
 * 7.获取每个分区的最大边界矩形，构建全局索引
 * @param <G>
 */
public class IndexablePCollection  <G extends Geometry> extends GeometryPCollection<G> implements  Serializable {
    private static final long serialVersionUID = 1L;

    protected transient PCollectionList<Indexable> indices=null;
    protected int indexType =0; //-1-不使用索引， 0-STRTree, 1-RTree, 2-QuadTree
    protected int indexCapacity;
    protected int leafCapacity;


    /**
     *
     * @param pp
     * @param partitions
     * @param indexType       -1-不使用索引， 0-STRTree, 1-RTree, 2-QuadTree
     * @param indexCapacity
     * @param leafCapacity
     */
    public IndexablePCollection(Preprocesser pp, int partitions, int indexType, int indexCapacity, int leafCapacity) {
        super(pp, partitions);
        this.indexType = indexType;
        this.indexCapacity = indexCapacity;
        this.leafCapacity = leafCapacity;
        this.indexing();
    }

    public IndexablePCollection(SpatialPipeline p,
                                String inputLocation,
                                FileDataSplitter splitter,
                                int indexType,
                                int indexCapacity,
                                int leafCapacity) {
        //step1 : load data from file
        super(p, inputLocation, splitter);
        this.indexType=indexType;
        this.indexCapacity=indexCapacity;
        this.leafCapacity=leafCapacity;
    }

    public IndexablePCollection(SpatialPipeline p, List<G> geometries) {
        super(p, geometries);
    }

    public IndexablePCollection(SpatialPipeline p, String inputLocation, FileDataSplitter splitter) {
        super(p, inputLocation, splitter);
    }

    public IndexablePCollection(Pipeline p, String inputLocation, FileDataSplitter splitter) {
        super(p, inputLocation, splitter);
    }

    public IndexablePCollection(Pipeline p, List<G> geometries ) {
        super(p,  geometries);
    }

    public IndexablePCollection(SpatialPipeline p) {
        super(p);
    }

    public IndexablePCollection(Pipeline p ) {
        super(p);
    }

    public IndexablePCollection(PCollection<G> p ) {
        super(p);
    }

    public int getIndexType() {
        return indexType;
    }

    public PCollectionList<Indexable> getIndices() {
        return indices;
    }

    public void indexing(){
        buildPartitionIndices();
    }
    /**
     * 对每个分区构建一个索引，新建的索引存放在this.indices中
     */
    protected PCollectionList<Indexable> buildPartitionIndices(){
        PCollectionList<G> pgs = this.getPartitionedGeometries();
        if(pgs!=null) {
            List<PCollection<G>>  ls =  pgs.getAll();
            ArrayList<PCollection<Indexable>> al = new ArrayList<>(pgs.size());
            for(PCollection<G> p : ls){
                PCollection<Indexable> pi= buildIndexFromGeometries(p);
                al.add(pi);
            }
            this.indices=PCollectionList.<Indexable>of(al);
            return this.indices;
        }
        else
            return null;
    }
    /**
     *
     * @param geometries
     * @return
     */
    protected PCollection<Indexable> buildIndexFromGeometries(PCollection<G> geometries){
        PCollection<Iterable<G>> pie = geometries
                .apply(IterablePTransforms.<G>toIterable());
        PCollection<Indexable> pt =pie
                .apply(ParDo.<Iterable<G>,Indexable>of(
                        new DoFn<Iterable<G>,Indexable>(){
            @ProcessElement
            public void processElement(ProcessContext c){
                Iterable<G> es = c.element();
                c.output((Indexable)buildIndexFromGeometries(es));
            }
        }));
        return pt;
    }

    protected Indexable buildIndexFromGeometries(Iterable<G> geometries){
        final ArrayList<G> al = new ArrayList<>();
        geometries.forEach(r->al.add(r));
        Object [] objects = al.toArray();
        return buildIndexFromGeometries(objects);
    }

    protected Indexable buildIndexFromGeometries(List<G> geometries){
        Object [] objects = geometries.toArray();
        return buildIndexFromGeometries(objects);
    }

    protected Indexable buildIndexFromGeometries(Object[] geometries){
        int type = getIndexType();
        if(type<0) return null;

        //这里传入的数据是存在于geometries中的，可能导致互斥访问问题
        //这里进行一个拷贝，避免互斥问题
        Object[] objects = new Object[geometries.length];
        for(int i=0;i<geometries.length;++i){
            objects[i]=((Geometry)geometries[i]).clone();
        }

        if(type==0){//STRTree
            STRTree strTree = STRTree.create(this.leafCapacity);
            for(int i=0;i<objects.length;++i)
                strTree.insert(((Geometry)(objects[i])).getEnvelope(),//Envelope
                        objects[i]);//Geometry
            return strTree;
        }
        else if(type==1) {//RTree
            RTree rTree = RTree.create(this.totalExtent.getDimension(),this.indexCapacity,this.leafCapacity);
            for(int i=0;i<objects.length;++i)
                rTree.insert((Geometry)objects[i]);
            return rTree;
        }
        else if(type==2){//QuadTree
            Envelope total = Envelope.create(this.totalExtent.getDimension());
            for(int i=0;i<objects.length;++i)
                total.combine((Envelope)objects[i]);

            QuadTree quadTree = QuadTree.create(this.leafCapacity,this.totalExtent.getDimension(),total);
            for(int i=0;i<objects.length;++i)
                quadTree.insert((Geometry)objects[i]);

            return quadTree;
        }
        else {
            try {
                throw new IllegalStateException("createGlobalIndex(Object[] objects): error index type");
            }
            catch (IllegalStateException e){
                e.printStackTrace();
            }
            return null;
        }
    }

    protected PCollection<Indexable> buildIndexFromEnvelopes(PCollection<Envelope> envelopePCollection){

        PCollection<Iterable<Envelope>> pie = envelopePCollection
                .apply(IterablePTransforms.<Envelope>toIterable());
        PCollection<Indexable> pt =pie
                .apply(ParDo.<Iterable<Envelope>,Indexable>of(
                        new DoFn<Iterable<Envelope>,Indexable>(){
                            @ProcessElement
                            public void processElement(ProcessContext c){
                                Iterable<Envelope> es = c.element();
                                c.output((Indexable)buildIndexFromEnvelopes(es));
                            }
                        }));
        return pt;
    }
    /**
     * 根据传入的矩形对象构建一个索引
     * @param envelopes
     * @return
     */
    protected Indexable buildIndexFromEnvelopes(Iterable<Envelope> envelopes){
        final ArrayList<Envelope> al = new ArrayList<>();
        envelopes.forEach(r->al.add(r));
        Object [] objects = al.toArray();
        return buildIndexFromEnvelopes(objects);
    }
    /**
     * 根据传入的矩形对象构建一个索引
     * @param envelopes  分区矩形列表
     * @return
     */
    protected Indexable buildIndexFromEnvelopes(List<Envelope> envelopes){
        Object [] objects = envelopes.toArray();
        return buildIndexFromEnvelopes(objects);
    }

    /**
     * 根据传入的对象构建一个索引
     * @param objects 矩形数组
     */
    protected Indexable buildIndexFromEnvelopes(Object[] objects){
        int type = getIndexType();
        if(type<0) return null;
        if(type==0){//STRTree
            STRTree strTree = STRTree.create(this.leafCapacity);
            for(int i=0;i<objects.length;++i)
                strTree.insert((Envelope) objects[i],//Envelope
                        Integer.valueOf(i));//Partition ID
            return strTree;
        }
        else if(type==1) {//RTree
            RTree rTree = RTree.create(this.totalExtent.getDimension(),this.indexCapacity,this.leafCapacity);
            for(int i=0;i<objects.length;++i)
                rTree.insert(null,new RegionShape((Envelope)objects[i]), Identifier.create((long)i));
            return rTree;
        }
        else if(type==2){//QuadTree
            Envelope total = Envelope.create(this.totalExtent.getDimension());
            for(int i=0;i<objects.length;++i)
                total.combine((Envelope)objects[i]);

            QuadTree quadTree = QuadTree.create(this.leafCapacity,this.totalExtent.getDimension(),total);
            for(int i=0;i<objects.length;++i)
                quadTree.insert(null,new RegionShape((Envelope)objects[i]), Identifier.create((long)i));

            return quadTree;
        }
        else {
            try {
                throw new IllegalStateException("buildIndexFromEnvelopes(Object[] objects): error index type");
            }
            catch (IllegalStateException e){
                e.printStackTrace();
            }
            return null;
        }
    }

    /**
     *  create A index by the geometries in the geometryCollection,
     *  that is the number of index in the result  is only one
     * @param geometryCollection
     * @param indexType
     * @param totalExtent
     * @param indexCapacity
     * @param leafCapacity
     * @param <G>
     * @param <T>
     * @return
     */
    protected static <G extends Geometry, T extends Indexable> PCollection<T> buildIndex(
            PCollection<G> geometryCollection,
            int indexType,
            Envelope totalExtent,
            int indexCapacity,
            int leafCapacity){
        return geometryCollection.apply(IndexPTransforms.createGeometriesToIndexCombiner(indexType,  totalExtent,  indexCapacity,  leafCapacity));
    }




//    protected void createGlobalIndexFromGeometries(List<Geometry> geoms){
//        Object [] objects = geoms.parallelStream().scene(r->r.getEnvelope()).toArray();
//        createGlobalIndex(objects);
//    }
//
//    protected void createGlobalIndexFromSamples(String samplePath){
//        List<String> ss = RandomSampler.loadSamples(samplePath);
//        final WKTReader wktReader=WKTReader.create();
//
//        Object [] objects = ss.parallelStream().scene(r->{
//            Geometry g = wktReader.read(r);
//            return g.getEnvelope();
//        }).toArray();
//
//        createGlobalIndex(objects);
//    }

    public void rangeQuery(final Envelope shape, final Visitor visitor){
        rangeQuery(shape,visitor,2);
    }

    /**
     *
     * @param shape
     * @param visitor
     * @param type 0 - PCollection 1-PCollectionList 2-Index
     */
    public void rangeQuery(final Envelope shape, final Visitor visitor, final int type){
        if(this.totalExtent.intersects(shape)==false)
            return;
        if(type<2){
            super.rangeQuery(shape,visitor,type);
        }
        else {

            for(int i=0;i<this.partitioningEnvelopes.size();++i){
                if(this.partitioningEnvelopes.get(i).intersects(shape)==false)
                    continue;
                this.indices.get(i).apply(ParDo.of(new DoFn<Indexable,Void>(){
                    @ProcessElement
                    public void processElement(ProcessContext context){
                        final RegionShape qshape = new RegionShape(shape);
                        context.element().intersects(qshape,visitor);
                    }
                }));
            }
        }
    }

}
