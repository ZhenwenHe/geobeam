package gtl.data.sdb.beam.feature;

import gtl.beam.coder.StorableCoder;
import gtl.beam.ptransform.FeaturePTransform;
import gtl.beam.ptransform.FeaturePTransforms;
import gtl.beam.ptransform.StringPTransforms;
import gtl.data.feature.FeatureReader;
import gtl.feature.Feature;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.values.PCollection;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class BeamFeatureReader implements FeatureReader {

    final PCollection<Feature> featurePCollection;

    public BeamFeatureReader(PCollection<Feature> featurePCollection) {
        this.featurePCollection = featurePCollection;
    }


    @Override
    public Feature read(int i, int i1) throws IOException {
        return null;
    }

    @Override
    public boolean hasNext() throws IOException {
        return false;
    }

    @Override
    public Feature next() throws IOException {
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

    public void print(){
        this.featurePCollection.setCoder(StorableCoder.of(Feature.class))
            .apply(FeaturePTransforms.wktPrinter());
    }

    public void writeWKT(String outputFile){
        this.featurePCollection
                .setCoder(StorableCoder.of(Feature.class))
                .apply(FeaturePTransforms.wktString())
                .setCoder(StringUtf8Coder.of())
                .apply(StringPTransforms.writer(outputFile));
    }

    public void writeTSV(String outputFile){
        this.featurePCollection
                .apply(FeaturePTransforms.tsvString())
                .setCoder(StringUtf8Coder.of())
                .apply(StringPTransforms.writer(outputFile));
    }

    public void writeCSV(String outputFile){
        this.featurePCollection
                .apply(FeaturePTransforms.csvString())
                .setCoder(StringUtf8Coder.of())
                .apply(StringPTransforms.writer(outputFile));
    }
}
