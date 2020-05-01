package cn.edu.cug.cs.gtl.data.sdb.beam.feature;

import cn.edu.cug.cs.gtl.data.feature.FeatureWriter;
import cn.edu.cug.cs.gtl.feature.Feature;
import org.apache.beam.sdk.values.PCollection;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class BeamFeatureWriter implements FeatureWriter {

    PCollection<Feature> featurePCollection;

    public BeamFeatureWriter(PCollection<Feature> featurePCollection) {
        this.featurePCollection = featurePCollection;
    }

    @Override
    public void write(Feature feature) throws IOException {

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
