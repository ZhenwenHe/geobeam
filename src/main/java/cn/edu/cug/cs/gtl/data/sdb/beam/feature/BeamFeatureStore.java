package cn.edu.cug.cs.gtl.data.sdb.beam.feature;

import cn.edu.cug.cs.gtl.beam.ptransform.StringPTransforms;
import cn.edu.cug.cs.gtl.common.Identifier;
import cn.edu.cug.cs.gtl.common.PropertySet;
import cn.edu.cug.cs.gtl.common.Variant;
import cn.edu.cug.cs.gtl.config.Config;
import cn.edu.cug.cs.gtl.data.feature.FeatureReader;
import cn.edu.cug.cs.gtl.data.feature.FeatureSet;
import cn.edu.cug.cs.gtl.data.feature.FeatureStore;
import cn.edu.cug.cs.gtl.data.feature.FeatureWriter;
import cn.edu.cug.cs.gtl.feature.Feature;
import cn.edu.cug.cs.gtl.feature.FeatureType;
import cn.edu.cug.cs.gtl.feature.FeatureTypeBuilder;
import cn.edu.cug.cs.gtl.feature.FeatureTypeFinder;
import cn.edu.cug.cs.gtl.geom.Geometry;
import cn.edu.cug.cs.gtl.io.*;
import cn.edu.cug.cs.gtl.io.File;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class BeamFeatureStore implements FeatureStore {

    List<FeatureSet> featureSets;
    PropertySet propertySet=null;
    String name;

    public BeamFeatureStore() {
        this.featureSets=new ArrayList<>();
        this.name=new String ("BeamFeatureStore");
    }

    public BeamFeatureStore(String name) {
        this.featureSets=new ArrayList<>();
        this.name=name;
    }

    @Override
    public boolean open(String path) throws IOException {
        return false;
    }

    @Override
    public void close() {
        this.featureSets=null;
    }

    @Override
    public FeatureTypeFinder getFeatureTypeFinder() {
        return new FeatureTypeFinderImpl();
    }

    @Override
    public FeatureSet createFeatureSet(FeatureType featureType) throws IOException {
        if(featureType==null) return null;
        FeatureTypeFinder ftf = getFeatureTypeFinder();
        FeatureType ft = ftf.find(featureType.getName());
        if(ft!=null){
            return getFeatureSet(featureType.getName());
        }
        else {
            FeatureSet fs= new BeamFeatureSet(this,featureType);
            featureSets.add(fs);
            return fs;
        }
    }

    public FeatureSet createFeatureSetFromTSVFile(Pipeline p, String inputFile) throws IOException, InterruptedException{
        final String schemaName = File.getFileNameWithoutSuffix(inputFile);
        final  String swapFileName = Config.getLocalSwapFile();
//        PCollection<FeatureType> pft = p.apply(TextIO.read().from(inputFile+".schema"))
//                .setCoder(StringUtf8Coder.of())
//                .apply(FeatureTypePTransforms.fromTSVString())
//                .setCoder(StorableCoder.of(FeatureType.class));
//
//                .apply(FeatureTypePTransforms.toByteString())
//                .setCoder(StringUtf8Coder.of())
//                .apply(StringPTransforms.writer(swapFileName)); //write to local swap file
//        p.run().waitUntilFinish();
        p.apply(TextIO.read().from(inputFile+".schema"))
                .setCoder(StringUtf8Coder.of())
                .apply(StringPTransforms.writer(swapFileName)); //write to local swap file
        p.run().waitUntilFinish();
        // wait until the swapFile exists
        File swapFile = new File(swapFileName);
        while (!swapFile.exists()) Thread.sleep(10);
        //read the local swap file and builder feature type
        BufferedReader bf= new BufferedReader(new FileReader(swapFile));
        String inputLine = bf.readLine().trim();
        bf.close();
        FeatureType ft = FeatureType.tsvString(inputLine);
        //List<FeatureType> lft = PCollectionUtils.collectFeatureTypes(pft);
        //if(lft==null || lft.size()==0) return null;
        //BeamFeatureSet bfs = new BeamFeatureSet(this,lft.get(0));
        BeamFeatureSet bfs = new BeamFeatureSet(this,ft);
        bfs.readTSV(p,inputFile);
        p.run().waitUntilFinish();
        featureSets.add(bfs);
        return bfs;
    }

    public FeatureSet createFeatureSetFromSSVFile(Pipeline p, String inputFile) throws IOException, InterruptedException{
        final String schemaName = File.getFileNameWithoutSuffix(inputFile);
        final  String swapFileName = Config.getLocalSwapFile();
        p.apply(TextIO.read().from(inputFile+".schema"))
                .setCoder(StringUtf8Coder.of())
                .apply(StringPTransforms.writer(swapFileName)); //write to local swap file
        p.run().waitUntilFinish();
        // wait until the swapFile exists
        File swapFile = new File(swapFileName);
        while (!swapFile.exists()) Thread.sleep(10);
        //read the local swap file and builder feature type
        BufferedReader bf= new BufferedReader(new FileReader(swapFile));
        String inputLine = bf.readLine().trim();
        bf.close();
        FeatureType ft = FeatureType.fromString(inputLine);
        BeamFeatureSet bfs = new BeamFeatureSet(this,ft);
        bfs.readSSV(p,inputFile);
        p.run().waitUntilFinish();
        featureSets.add(bfs);
        return bfs;
    }

    public FeatureSet createFeatureSetFromCSVFile(Pipeline p, String inputFile) throws IOException, InterruptedException{
        final String schemaName = File.getFileNameWithoutSuffix(inputFile);
        final  String swapFileName = Config.getLocalSwapFile();
        p.apply(TextIO.read().from(inputFile+".schema"))
                .setCoder(StringUtf8Coder.of())
                .apply(StringPTransforms.writer(swapFileName)); //write to local swap file
        p.run().waitUntilFinish();
        // wait until the swapFile exists
        File swapFile = new File(swapFileName);
        while (!swapFile.exists()) Thread.sleep(10);
        //read the local swap file and builder feature type
        BufferedReader bf= new BufferedReader(new FileReader(swapFile));
        String inputLine = bf.readLine().trim();
        bf.close();
        FeatureType ft = FeatureType.fromString(inputLine);
        BeamFeatureSet bfs = new BeamFeatureSet(this,ft);
        bfs.readCSV(p,inputFile);
        p.run().waitUntilFinish();
        featureSets.add(bfs);
        return bfs;
    }

    @Override
    public FeatureSet getFeatureSet(String s) throws IOException {
        for(FeatureSet featureSet: featureSets)
            if(s.equals(featureSet.getName()))
                return featureSet;
        return null;
    }

    @Override
    public boolean removeFeatureSet(String s) throws IOException {
        return false;
    }

    @Override
    public Feature appendFeature(Feature feature) throws IOException {
        return null;
    }

    @Override
    public Feature removeFeature(Feature feature) throws IOException {
        return null;
    }

    @Override
    public FeatureWriter getFeatureWriter(String s) throws IOException {
        return null;
    }

    @Override
    public FeatureReader getFeatureReader(String s) throws IOException {
        return null;
    }

    @Override
    public PropertySet getProperties() {
        return this.propertySet;
    }

    @Override
    public void setProperties(PropertySet propertySet) {
        this.propertySet=propertySet;
    }

   /* @Override
    public ServiceInfo getInfo() {
        return null;
    }*/

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public void createSchema(FeatureType featureType) throws IOException {
        createFeatureSet(featureType);
    }

    /*@Override
    public void updateSchema(String s, FeatureType featureType) throws IOException {

    }*/

    @Override
    public void removeSchema(String s) throws IOException {

    }

    @Override
    public String[] getSchemaNames() throws IOException {
        ArrayList<String> als = new ArrayList<>(this.featureSets.size());
        try {
            for(FeatureSet fs : featureSets){
                als.add(fs.getFeatureType().getName()) ;
            }
        }
        catch (IOException e){
            e.printStackTrace();
        }
        return als.toArray(new String[als.size()]);
    }

    @Override
    public List<FeatureType> getSchemas() throws IOException {
        ArrayList<FeatureType> als = new ArrayList<>(this.featureSets.size());
        try {
            for(FeatureSet fs : featureSets){
                als.add(fs.getFeatureType()) ;
            }
        }
        catch (IOException e){
            e.printStackTrace();
        }
        return als;
    }

    @Override
    public DataSet<FeatureType, Feature> getDataSet(String s) throws IOException {
        return getFeatureSet(s);
    }

    @Override
    public Object clone() {
        return null;
    }

    @Override
    public boolean load(DataInput dataInput) throws IOException {
        return false;
    }

    @Override
    public boolean store(DataOutput dataOutput) throws IOException {
        return false;
    }

    class FeatureTypeFinderImpl implements FeatureTypeFinder{
        @Override
        public FeatureType find(String s)  {
            try {
                for(FeatureSet fs : featureSets){
                    if(fs.getFeatureType().getName().equals(s)){
                        return fs.getFeatureType();
                    }
                }
            }
            catch (IOException e){
                e.printStackTrace();
            }
            return null;
        }

        @Override
        public FeatureType find(Identifier s) {
            try {
                for(FeatureSet fs : featureSets){
                    if(fs.getFeatureType().getIdentifier().equals(s)){
                        return fs.getFeatureType();
                    }
                }
            }
            catch (IOException e){
                e.printStackTrace();
            }
            return null;
        }


    }
}
