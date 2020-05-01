package cn.edu.cug.cs.gtl.data.sdb.beam;

import cn.edu.cug.cs.gtl.data.DataSolution;
import cn.edu.cug.cs.gtl.data.sdb.beam.feature.BeamFeatureStore;
import cn.edu.cug.cs.gtl.common.PropertySet;
import cn.edu.cug.cs.gtl.data.DataSolution;
import cn.edu.cug.cs.gtl.data.feature.FeatureStore;
import cn.edu.cug.cs.gtl.data.material.MaterialStore;
import cn.edu.cug.cs.gtl.data.styling.StyleStore;
import cn.edu.cug.cs.gtl.data.texture.TextureStore;
import cn.edu.cug.cs.gtl.feature.Feature;
import cn.edu.cug.cs.gtl.feature.FeatureType;
import cn.edu.cug.cs.gtl.io.DataStore;
import org.apache.beam.sdk.Pipeline;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

/**
 * 基于Beam的数据方案
 */
public class BeamDataSolution implements DataSolution {

    List<FeatureStore> featureStores;
    MaterialStore materialStore=null;
    TextureStore textureStore=null;
    StyleStore styleStore=null;
    PropertySet propertySet=null;
    Pipeline pipeline=null;

    public BeamDataSolution(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    @Override
    public boolean open(String path) throws IOException {
        featureStores=new ArrayList<>();
        return true;
    }

    @Override
    public void close() {
        featureStores=null;
    }

    @Override
    public FeatureStore createFeatureStore(String s) throws IOException {
        FeatureStore fs = getFeatureStore(s);
        if(fs==null){
            fs = new BeamFeatureStore(s);
        }
        return fs;
    }

    @Override
    public FeatureStore getFeatureStore(String s) throws IOException {
        if(featureStores==null) return null;
        for(FeatureStore fs : featureStores){
            if(fs.getName().equals(s))
                return fs;
        }
        return null;
    }

    @Override
    public List<FeatureStore> getFeatureStores() throws IOException {
        return this.featureStores;
    }

    @Override
    public MaterialStore getMaterialStore() throws IOException {
        return this.materialStore;
    }

    @Override
    public TextureStore getTextureStore() throws IOException {
        return this.textureStore;
    }

    @Override
    public StyleStore getStyleStore() throws IOException {
        return this.styleStore;
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
    public DataStore<FeatureType, Feature> createDataStore(String s) throws IOException {
        return createFeatureStore(s);
    }

    @Override
    public DataStore<FeatureType, Feature> getDataStore(String s) throws IOException {
        return getFeatureStore(s);
    }

    @Override
    public URI getURI() throws IOException {
        return null;
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
}
