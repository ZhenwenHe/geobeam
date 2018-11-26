package gtl.data.sdb.beam.feature;

import gtl.beam.ptransform.*;
import gtl.beam.utils.PCollectionUtils;
import gtl.common.Identifier;
import gtl.common.PropertySet;
import gtl.data.feature.FeatureReader;
import gtl.data.feature.FeatureSet;
import gtl.data.feature.FeatureStore;
import gtl.data.feature.FeatureWriter;
import gtl.feature.Feature;
import gtl.feature.FeatureBuilder;
import gtl.feature.FeatureType;
import gtl.beam.coder.StorableCoder;
import gtl.geom.Envelope;
import gtl.geom.Geometry;
import gtl.io.*;
import gtl.io.Filter;
import gtl.io.wkt.WKTReader;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class BeamFeatureSet implements FeatureSet {

    protected static final Logger LOG = LoggerFactory.getLogger(BeamFeatureSet.class);

    PCollection<Feature> featurePCollection;
    Envelope totalEnvelope=null;
    final FeatureType featureType;
    final FeatureStore featureStore;
    PropertySet propertySet;


    public BeamFeatureSet(FeatureStore featureStore,FeatureType featureType) {
        this.featureType = featureType;
        this.featureStore = featureStore;
        this.propertySet=null;
    }

    @Override
    public FeatureWriter getFeatureWriter() throws IOException {
        return new BeamFeatureWriter(this.featurePCollection);
    }

    @Override
    public FeatureReader getFeatureReader() throws IOException {
        return new BeamFeatureReader(this.featurePCollection);
    }

    @Override
    public FeatureType getFeatureType() throws IOException {
        return featureType;
    }


    @Override
    public PropertySet getProperties() {
        return this.propertySet;
    }

    @Override
    public void setProperties(PropertySet propertySet) {
        this.propertySet=propertySet;
    }

    @Override
    public String getName() {
        return this.featureType.getName();
    }

    @Override
    public FeatureType getSchema() throws IOException {
        return this.featureType;
    }

    @Override
    public List<Feature> getData(Filter<Feature> filter) throws IOException {
        PCollection<Feature> pc = this.featurePCollection.apply(FeaturePTransform.by(filter));
        //pc.apply(FeaturePTransforms.wktPrinter());
        return PCollectionUtils.collectFeatures(pc);
    }

    @Override
    public DataWriter<Feature> getDataWriter() throws IOException {
        return null;
    }

    @Override
    public DataReader<Feature> getDataReader(Filter<Feature> filter) throws IOException {
        PCollection<Feature> pc = this.featurePCollection
                .apply(FeaturePTransform.by(filter))
                .setCoder(StorableCoder.of(Feature.class));
        return new BeamFeatureReader(pc);
    }


    @Override
    public Object clone() {
        BeamFeatureSet b  = new BeamFeatureSet(this.featureStore,this.featureType);
        if(this.propertySet!=null)
            b.propertySet=(PropertySet)this.propertySet.clone();
        b.featurePCollection=this.featurePCollection;
        return b;
    }

    @Override
    public boolean load(DataInput dataInput) throws IOException {
        return false;
    }

    @Override
    public boolean store(DataOutput dataOutput) throws IOException {
        return false;
    }


    /**
     * 加载TSV文件
     * @param p
     * @param inputDataFile
     * @throws IOException
     */
    void readTSV(Pipeline p , String inputDataFile) throws IOException{
        final FeatureType ft = getFeatureType();
        final Class<?>  geomType =ft.getGeometryDescriptor().getGeometryType().getBinding();
        this.featurePCollection=p.apply(FeaturePTransforms.tsvReader(inputDataFile))
                .setCoder(StringUtf8Coder.of())
                .apply(FeaturePTransforms.tsvFilter(geomType))
                .setCoder(StringUtf8Coder.of())
                .apply(FeaturePTransforms.parserTSV(ft))
                .setCoder(StorableCoder.of(Feature.class));
    }

    /**
     * 加载CSV文件
     * @param p
     * @param inputDataFile
     * @throws IOException
     */
    void readCSV(Pipeline p , String inputDataFile) throws IOException{
        final FeatureType ft = getFeatureType();
        this.featurePCollection=p.apply(FeaturePTransforms.csvReader(inputDataFile))
                .setCoder(StringUtf8Coder.of())
                .apply(FeaturePTransforms.csvFilter(ft.getGeometryDescriptor().getGeometryType().getType()))
                .setCoder(StringUtf8Coder.of())
                .apply(FeaturePTransforms.parserCSV(ft))
                .setCoder(StorableCoder.of(Feature.class));
    }

    /**
     * 加载SSV文件
     * @param p
     * @param inputDataFile
     * @throws IOException
     */
    void readSSV(Pipeline p , String inputDataFile) throws IOException{
        final FeatureType ft = getFeatureType();
        this.featurePCollection=p.apply(FeaturePTransforms.ssvReader(inputDataFile))
                .setCoder(StringUtf8Coder.of())
                .apply(FeaturePTransforms.ssvFilter(ft.getGeometryDescriptor().getGeometryType().getType()))
                .setCoder(StringUtf8Coder.of())
                .apply(FeaturePTransforms.parserSSV(ft))
                .setCoder(StorableCoder.of(Feature.class));
    }
    /**
     * 获取数据集的最小边界矩形
     * @return
     */
    public  Envelope getTotalExtent( ){
        if(this.totalEnvelope==null) {
            PCollection<Envelope> pe =  calculateEnvelope();
            this.totalEnvelope= PCollectionUtils.collectEnvelopes(pe).get(0);
            pe.getPipeline().run().waitUntilFinish();
            return this.totalEnvelope;
        }
        else
            return this.totalEnvelope;
    }

    /**
     * 计算数据集的最小边界矩形
     * @return
     */
    public PCollection<Envelope> calculateEnvelope(){
        PCollection<Envelope> pe = this.featurePCollection
                .apply(EnvelopePTransforms.fromFeature())
                .setCoder(StorableCoder.of(Envelope.class))
                .apply(EnvelopePTransforms.combiner())
                .setCoder(StorableCoder.of(Envelope.class));
        return pe;
    }

    /**
     * 计算数据元素个数
     * @return
     */
    public PCollection<Long> calculateCounter() {
        return this.featurePCollection
                .apply(FeaturePTransforms.counter());
    }

    /**
     *
     * @param samplesNumber
     * @return
     */
    public   PCollection<Feature>  anySampling(final long samplesNumber){
        return this.featurePCollection
                .apply(SampleTransforms.any(samplesNumber))
                .setCoder(StorableCoder.of(Feature.class));
    }
    /**
     * use side-input method to implement containsQuery
     * but there are something wrong in this function
     * due to serialization
     * @param r
     * @return
     */
    public PCollection<Feature> containsQuery(PCollection<Envelope> r){
        final PCollectionView<Envelope> ev = r.apply(EnvelopePTransforms.combiner(2).asSingletonView());
        return this.featurePCollection
                .apply(ParDo.of(new DoFn_containsQuery(ev)).withSideInputs(ev))
                .setCoder(this.featurePCollection.getCoder());
    }

    @DefaultCoder(StorableCoder.class)
    class DoFn_containsQuery extends DoFn<Feature,Feature> implements Serializable {
        private  final static long serialVersionUID = 0L;

        final PCollectionView<Envelope> pCollectionView;
        public DoFn_containsQuery(final PCollectionView<Envelope> pCollectionView){
            this.pCollectionView=pCollectionView;
        }
        @ProcessElement
        public void processElement(ProcessContext c){
            Envelope sideInputEnvelope = c.sideInput(pCollectionView);
            Feature f = c.element();
            if(sideInputEnvelope.contains(f.getGeometry().getEnvelope())){
                c.output(c.element());
            }
        }
    }
}
