package cn.edu.cug.cs.gtl.beam.pcollection;

import cn.edu.cug.cs.gtl.beam.partitioning.SpatialPartitioner;
import cn.edu.cug.cs.gtl.beam.partitioning.SpatialPartitioning;
import cn.edu.cug.cs.gtl.beam.pipeline.SpatialPipeline;
import cn.edu.cug.cs.gtl.beam.ptransform.IterablePTransforms;
import cn.edu.cug.cs.gtl.beam.ptransform.SampleTransforms;
import cn.edu.cug.cs.gtl.beam.utils.PCollectionUtils;
import cn.edu.cug.cs.gtl.geom.*;
import cn.edu.cug.cs.gtl.index.Visitor;
import cn.edu.cug.cs.gtl.io.File;
import cn.edu.cug.cs.gtl.io.FileDataSplitter;
import cn.edu.cug.cs.gtl.io.wkt.WKTReader;
import cn.edu.cug.cs.gtl.io.wkt.WKTWriter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.LocalResources;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

//Geometry PCollection
public class GeometryPCollection<T extends Geometry > implements PValue,Serializable{
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(GeometryPCollection.class);

    protected transient PCollection<T>  geometries;
    protected transient PCollectionList<T> partitionedGeometries;
    protected final transient Pipeline pipeline;

    protected List<Envelope>   partitioningEnvelopes=null;
    protected Envelope totalExtent=null;//total extent of the geometries
    protected Long totalNumber=null;// total number of the geometries


    public GeometryPCollection(Preprocesser pp, int partitions) {
        pipeline=pp.pipeline.getPipeline();
        totalNumber=pp.getTotalCount();
        totalExtent=pp.getTotalExtent();
        List<Geometry> samples = pp.getSampleList();
        ArrayList<Envelope> sampleEnvelopes = new ArrayList<>(samples.size());
        for(Geometry g: samples){
            sampleEnvelopes.add(g.getEnvelope());
        }
        this.partitioningEnvelopes=new ArrayList<Envelope>();
        Collection<Envelope> ces = SpatialPartitioning.createPartitioning(pp.getPartitionType(),
                totalExtent,
                sampleEnvelopes,
                partitions
        ).getPartitionEnvelopes();
        this.partitioningEnvelopes.addAll(ces);

        String fileType = File.getSuffixName(pp.getInputDataFile());

        if(fileType.equals("csv")){
            loadFromCSVFile(pp.getOutputDataFile());
        }
        else if(fileType.equals("tsv")){
            loadFromTSVFile(pp.getOutputDataFile());
        }
        else if(fileType.equals("shp")){
            loadFromSHPFile(pp.getOutputDataFile());
        }
        else{
            try {
                throw new IllegalArgumentException("wrong file type in GeometryPCollection");
            }
            catch (IllegalStateException e){
                e.printStackTrace();
            }
        }
        this.partitioning(this.partitioningEnvelopes);
    }

    public GeometryPCollection(SpatialPipeline p, List<T> geometries) {
        pipeline=p.getPipeline();
        this.geometries = p.apply(Create.<T>of(geometries));
        partitionedGeometries=null;
    }

    public GeometryPCollection(SpatialPipeline p,String inputLocation, FileDataSplitter splitter){
        this(p.getPipeline(),inputLocation,splitter);
        partitionedGeometries=null;
    }

    public GeometryPCollection(Pipeline p,String inputLocation, FileDataSplitter splitter){
        this.pipeline=p ;
        partitionedGeometries=null;
        if(splitter.equals(FileDataSplitter.TSV)){
            loadFromTSVFile(inputLocation);
        }
        else if(splitter.equals(FileDataSplitter.CSV)){
            loadFromCSVFile(inputLocation);
        }
        else{
            loadFromSHPFile(inputLocation);
        }
    }

    public GeometryPCollection(Pipeline p, List<T> geometries) {
        pipeline=p;
        this.geometries = p.apply(Create.<T>of(geometries));
        partitionedGeometries=null;
    }

    public GeometryPCollection(SpatialPipeline p ) {
        pipeline=p.getPipeline();
        this.geometries = null;
        partitionedGeometries=null;
    }

    public GeometryPCollection(Pipeline p ) {
        pipeline=p;
        this.geometries = null;
        partitionedGeometries=null;
    }

    public GeometryPCollection(PCollection<T> p ) {
        try {
            if(p==null)
                throw new NullPointerException("GeometryPCollection(PCollection<T> p )");
        }
        catch (NullPointerException e){
            e.printStackTrace();
        }
        pipeline=p.getPipeline();
        this.geometries = p;
        partitionedGeometries=null;
    }

    public PCollection<T> getGeometries() {
        return geometries;
    }

    public PCollectionList<T> getPartitionedGeometries() {
        return partitionedGeometries;
    }

    public Envelope getTotalExtent() {
        if(this.totalExtent==null)
            calculateTotalExtent();
        return this.totalExtent;
    }

    public Long getTotalNumber() {
        if(this.totalNumber==null)
            calculateTotalNumber();
        return this.totalNumber;
    }

    public  EnvelopePCollection  getEnvelopes(){
        PCollection<Envelope> es =geometries.apply(MapElements.via(new SimpleFunction<T, Envelope>() {
            @Override
            public Envelope apply(T g){
                return g.getEnvelope();
            }
        }));
        return new EnvelopePCollection(es);
    }

    public PCollection<Long> count(){
        return this.getGeometries().apply(Count.globally());
//        PCollection<Long> pl= geometries.apply(MapElements.via(new SimpleFunction<T, Long>() {
//            @Override
//            public Long apply(T g){
//                return 1L;
//            }
//        })).apply(Sum.longsGlobally());
//        return pl;
    }

    protected void calculateTotalExtent(){
        List<Envelope> c = PCollectionUtils.collect(getEnvelopes().combine().getEnvelopes());
        this.totalExtent = c.get(0);
    }

    protected void calculateTotalNumber(){
        this.totalNumber=PCollectionUtils.collect(this.count()).get(0);
    }

    @Override
    public String getName() {
        return this.getGeometries().getName();
    }

    @Override
    public Pipeline getPipeline() {
        return pipeline;
        //return this.geometries.getPipeline();
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
        return this.geometries.expand();
    }

    @Override
    public void finishSpecifyingOutput(String transformName, PInput input, PTransform<?, ?> transform) {
        this.getGeometries().finishSpecifyingOutput(transformName,input,transform);
    }

    @Override
    public void finishSpecifying(PInput upstreamInput, PTransform<?, ?> upstreamTransform) {
        this.getGeometries().finishSpecifying(upstreamInput,upstreamTransform);
    }

    /**
     * Like {@link PCollection.IsBounded#apply(String, PTransform)} but defaulting to the name
     * of the {@link PTransform}.
     *
     * @return the output of the applied {@link PTransform}
     */
    public <OutputT extends POutput> OutputT apply(PTransform<? super PCollection<T>, OutputT> t) {
        return Pipeline.applyTransform(this.getGeometries(), t);
    }

    /**
     * Applies the given {@link PTransform} to this input {@link PCollection},
     * using {@code name} to identify this specific application of the transform.
     * This name is used in various places, including the monitoring UI, logging,
     * and to stably identify this application node in the job graph.
     *
     * @return the output of the applied {@link PTransform}
     */
    public <OutputT extends POutput> OutputT apply(
            String name, PTransform<? super PCollection<T>, OutputT> t) {
        return Pipeline.applyTransform(name, this.getGeometries(), t);
    }
    /**
     *
     */
    public void print(){
        final WKTWriter wktWriter = WKTWriter.create(2);
        this.geometries.apply(ParDo.of(new DoFn<T, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c){
                T pp = (T) c.element();
                System.out.println(wktWriter.write(pp));
            }
        }));
    }

    public PCollection<String> filter(String inputDataFile, final int geomType){
        Pipeline p = getPipeline();

        LOG.debug(inputDataFile);
        PCollection<String> ps= p.apply( TextIO.read().from(inputDataFile)).apply(
                Filter.by(new SerializableFunction<String,Boolean>() {
                    @Override
                    public Boolean apply(String line){
                        int i = line.indexOf('(');
                        String tag = line.substring(0,i);
                        LOG.debug(tag);
                        return tag.trim().equalsIgnoreCase(Geometry.getSimpleTypeName(geomType)) ;
                    }
                })
        ).setCoder(StringUtf8Coder.of());
        return ps;
    }


    public boolean loadFromCSVFile(String fileName,final int geomType){
        final WKTReader wktReader = WKTReader.create();
        this.geometries = filter(fileName,geomType).apply(
                MapElements.via(new SimpleFunction<String, T>() {
                    @Override
                    public T apply(String line){
                        String[] columns = line.split(FileDataSplitter.CSV.getDelimiter());
                        T g=  (T)wktReader.read(columns[0]);
                        return  g;
                    }
                })
        );

        return true;
    }

    public boolean loadFromTSVFile(String fileName,final int geomType){
        Pipeline p = getPipeline();
        final WKTReader wktReader = WKTReader.create();
        this.geometries =filter(fileName,geomType).apply(
                MapElements.via(new SimpleFunction<String, T>() {
                    @Override
                    public T apply(String line){
                        String[] columns = line.split(FileDataSplitter.TSV.getDelimiter());
                        T g=  (T)wktReader.read(columns[0]);
                        return  g;
                    }
                })
        );
        return true;
    }

    public boolean loadFromSHPFile(String fileName,final int geomType){
        //final  List<T> ga = (List<T>) Geom2DSuits.extractGeometryFromShapeFile(fileName);
        //this.geometries = pipeline.apply(Create.<T>of(ga));
        return true;
    }

    /**
     *
     * @param fileName
     * @return
     */
    public boolean loadFromCSVFile(String fileName){
        Pipeline p = getPipeline();
        final WKTReader wktReader = WKTReader.create();

        this.geometries = p.apply("ReadLines", TextIO.read().from(fileName)).apply(
                MapElements.via(new SimpleFunction<String, T>() {
                    @Override
                    public T apply(String line){
                        String[] columns = line.split(FileDataSplitter.CSV.getDelimiter());
                        T g=  (T)wktReader.read(columns[0]);
                        return  g;
                    }
                })
        );
        return true;
    }


    public boolean loadFromTSVFile(String fileName){
        Pipeline p = getPipeline();
        final WKTReader wktReader = WKTReader.create();
        this.geometries = p.apply("ReadLines", TextIO.read().from(fileName)).apply(
                MapElements.via(new SimpleFunction<String, T>() {
                    @Override
                    public T apply(String line){
                        String[] columns = line.split(FileDataSplitter.TSV.getDelimiter());
                        T g=  (T)wktReader.read(columns[0]);
                        return  g;
                    }
                })
        );
        return true;
    }
    /**
     *
     * @param fileName
     * @return
     */
    public boolean loadFromSHPFile(String fileName){
        //final  List<T> ga = (List<T>) Geom2DSuits.extractGeometryFromShapeFile(fileName);
        //this.geometries = pipeline.apply(Create.<T>of(ga));
        return true;
    }

    public boolean storeToTSVFile(String fileName){
        Pipeline p = getPipeline();
        final WKTWriter wktWriter = WKTWriter.create(2);
        this.geometries.apply(
                MapElements.via(new SimpleFunction<T, String>() {
                    @Override
                    public String apply( T g){
                        return wktWriter.write(g);
                    }
                })
        ).apply(TextIO.write().to(fileName).withoutSharding());
        return true;
    }






    /**
     * 随机采样
     * @param samplesNumber
     * @return
     */
    public   PCollection<Iterable<T> >  sampling(final int samplesNumber){
        return this.geometries.apply(SampleTransforms.fixedSizeGlobally(samplesNumber));
    }

    /**
     *
     * @param samplesNumber
     * @return
     */
    protected  PCollection<T>  anySampling(final int samplesNumber){
        return this.geometries.apply(SampleTransforms.any(samplesNumber));
    }
    /**
     *  sampling and write the results to the sampleFilesPath
     * @param samplesNumber
     * @param sampleFilesPath
     * @return
     */
    public ResourceId sampling (final int samplesNumber,String sampleFilesPath){
        final WKTWriter wktWriter =  WKTWriter.create(2);
        PCollection<Iterable<T> > pc =  this.geometries.apply(SampleTransforms.fixedSizeGlobally(samplesNumber));
        PCollection<String> pcs= pc.apply(ParDo.<Iterable<T>,String>of(new DoFn<Iterable<T>, String>() {
            @ProcessElement
            public void processElement(ProcessContext c){
                Iterable<T> gi = c.element();
                for (T geometry : gi) {
                    c.output(wktWriter.write(geometry));
                }
            }
        }));

        ResourceId lr = LocalResources.fromString(sampleFilesPath,true);
        pcs.apply(TextIO.write().to(lr));

        return lr;
    }

    /**
     *  sampling and write the results to the sampleFile
     * @param samplesNumber
     * @param sampleFile
     * @return
     */
    public ResourceId sampling (final int samplesNumber,ResourceId sampleFile){
        final WKTWriter wktWriter =  WKTWriter.create(2);
        PCollection<Iterable<T> > pc =  this.geometries.apply(SampleTransforms.fixedSizeGlobally(samplesNumber));
        PCollection<String> pcs= pc.apply(ParDo.<Iterable<T>,String>of(new DoFn<Iterable<T>, String>() {
            @ProcessElement
            public void processElement(ProcessContext c){
                Iterable<T> gi = c.element();
                for (T geometry : gi) {
                    c.output(wktWriter.write(geometry));
                }
            }
        }));

        pcs.apply(TextIO.write().to(sampleFile).withoutSharding());

        return sampleFile;
    }

    /**
     * partitioning
     * @param sp
     * @param <E>
     * @return
     */
    public <E extends Envelope> PCollectionList<T> partitioning(SpatialPartitioning<E> sp){
        this.partitionedGeometries= this.getGeometries().apply(
                SpatialPartitioner.<Envelope,T>createPartitionTransform(
                        sp.getPartitionEnvelopes()));
        return this.partitionedGeometries;
    }

    /**
     * partitioning the geometries according to partitioningEnvelopes
     * @param partitioningEnvelopes
     */
    public  void partitioning(List<Envelope> partitioningEnvelopes) {
        this.partitioningEnvelopes = partitioningEnvelopes;
        this.totalExtent=(Envelope) partitioningEnvelopes.get(0).clone();
        for(Envelope e: this.partitioningEnvelopes)
            this.totalExtent.combine(e);
        this.partitionedGeometries= this.getGeometries().apply(
                SpatialPartitioner.<Envelope,T>createPartitionTransform(
                        this.partitioningEnvelopes));
    }
    /**
     *
     * @param type the type of SpatialPartitioning
     *               public static final int EQUAL_PARTITIONING=1;
     *               public static final int HILBERT_PARTITIONING=2;
     *               public static final int KDTREE_PARTITIONING=3;
     *               public static final int QUADTREE_PARTITIONING=4;
     *               public static final int RTREE_PARTITIONING=5;
     *               public static final int STRTREE_PARTITIONING=6;
     *               public static final int VORONOI_PARTITIONING=7;
     * @param samples
     *              if the type == EQUAL_PARTITIONING, the samples is not nessecary
     * @param partitionNumber
     * @param <E>
     * @return
     */
    protected <E extends Envelope> PCollectionList<T> partitioning(int type,PCollection<E> samples, int partitionNumber) {

        PCollection<SpatialPartitioning<E>> psp = createSpatialPartitioning(type,samples,partitionNumber);
        PCollection<Envelope> envelopes = psp.apply(ParDo.of(new DoFn<SpatialPartitioning<E>, Iterable<Envelope>>() {
            @ProcessElement
            public void processElement(ProcessContext c){
                SpatialPartitioning<E> sp = c.element();
                c.output(sp.getPartitionEnvelopes());
            }
        })).apply(Flatten.iterables());
        final Collection<Envelope> localEnvelopes = PCollectionUtils.collect(envelopes);
        this.partitionedGeometries= this.getGeometries().apply(
                SpatialPartitioner.<Envelope,T>createPartitionTransform(
                        localEnvelopes));
        return this.partitionedGeometries;
    }



    /**
     *
     * @param type the type of SpatialPartitioning
     *               public static final int EQUAL_PARTITIONING=1;
     *               public static final int HILBERT_PARTITIONING=2;
     *               public static final int KDTREE_PARTITIONING=3;
     *               public static final int QUADTREE_PARTITIONING=4;
     *               public static final int RTREE_PARTITIONING=5;
     *               public static final int STRTREE_PARTITIONING=6;
     *               public static final int VORONOI_PARTITIONING=7;
     * @param samples
     * @param partitionNumber
     * @param <E>
     * @return
     */
    protected <E extends Envelope> PCollection<SpatialPartitioning<E>> createSpatialPartitioning(
            int type,PCollection<E> samples, int partitionNumber){

        final PCollectionView<Iterable<Integer>> intsView = getPipeline()
                .apply(Create.<Integer>of(Arrays.asList(type,partitionNumber)))
                .apply(IterablePTransforms.<Integer>toIterable().asSingletonView());
        //final PCollectionView<Iterable<E>> samplesView=samples.apply(IterablePTransforms.<E>toIterable().asSingletonView());
//        final PCollectionView<Iterable<Envelope>> totalExtentView= getPipeline()
//                        .apply(Create.<Envelope>of(Arrays.asList(getTotalExtent())))
//                        .apply(IterablePTransforms.<Envelope>toIterable().asSingletonView());

        PCollection<SpatialPartitioning<E>> r = samples
                .apply(IterablePTransforms.<E>toIterable())
                .apply(ParDo.of(new DoFn<Iterable<E>, SpatialPartitioning<E>>() {
            @ProcessElement
            public void processElement(ProcessContext c){
                Iterator<Integer> ints = c.sideInput(intsView).iterator();
                int type =ints.next().intValue();
                int partitionNumber =ints.next().intValue();
                Iterable<E> envelopes = c.element();
                E totalEnvelope=(E)envelopes.iterator().next().clone();
                for(E e: envelopes){
                    totalEnvelope.combine((Envelope) e);
                }
                SpatialPartitioning<E> sp =
                        SpatialPartitioning.<E>createPartitioning(type,
                                (E)totalEnvelope,c.element(),partitionNumber);
                c.output(sp);
            }
        }).withSideInputs(intsView));

        return r;
    }

    public void rangeQuery(final Envelope shape, final Visitor visitor){
          rangeQuery(shape,visitor,0);
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
        if(type==0){
            this.geometries.apply(ParDo.of(new DoFn<T,Void>(){
                @ProcessElement
                public void processElement(ProcessContext context){
                    if(shape.intersects(context.element().getEnvelope())) {
                        visitor.visitGeometry(context.element());
                    }
                }
            }));
        }
        else if(type==1){
            for(int i=0;i<this.partitioningEnvelopes.size();++i){
                if(this.partitioningEnvelopes.get(i).intersects(shape)==false)
                    continue;
                this.partitionedGeometries.get(i).apply(ParDo.of(new DoFn<T,Void>(){
                    @ProcessElement
                    public void processElement(ProcessContext context){
                        if(shape.intersects(context.element().getEnvelope()))
                            visitor.visitGeometry(context.element());
                    }
                }));
            }
        }
    }
}
