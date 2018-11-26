package gtl.beam.ptransform;


import gtl.beam.coder.StorableCoder;
import gtl.feature.Feature;
import gtl.io.Filter;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

import javax.annotation.Nullable;

/**
 * <code>
 *     PCollection<Feature> pfs = p.apply(FeaturePTransforms.tsvReader(inputDataFile))
 *                 .setCoder(StringUtf8Coder.of())
 *                 .apply(FeaturePTransforms.tsvFilter(geomType))
 *                 .setCoder(StringUtf8Coder.of())
 *                 .apply(FeaturePTransforms.parser(FileDataSplitter.TSV.getDelimiter(),ft))
 *                 .setCoder(StorableCoder.of(Feature.class));
 *
 *         pfs.apply(FeaturePTransforms.geometry())
 *                 .setCoder(GeometryCoder.of(Geometry.class))
 *                 .apply(GeometryPTransforms.wktString())
 *                 .setCoder(StringUtf8Coder.of())
 *                 .apply(StringPTransforms.writer(outputDataFile+"1.wkt"));
 *
 *         pfs.apply(FeaturePTransforms.tsvString())
 *                 .setCoder(StringUtf8Coder.of())
 *                 .apply(StringPTransforms.writer(outputDataFile+".tsv"));
 * </code>
 */

public class FeaturePTransform  extends GeneralPTransform< Feature> {
    private final Filter<Feature> predicate;

    public static FeaturePTransform by(Filter<Feature> predicate) {
        return new FeaturePTransform(predicate);
    }

    private FeaturePTransform(Filter<Feature> predicate) {
        super();
        this.predicate=predicate;
    }

    private FeaturePTransform(@Nullable String name,Filter<Feature> predicate) {
        super(name);
        this.predicate=predicate;
    }

    @Override
    public PCollection<Feature> expand(PCollection<Feature> input) {
        return input
                .apply(ParDo.of(new FeatureDoFn()))
                .setCoder(input.getCoder());
    }

    @DefaultCoder(StorableCoder.class)
    class FeatureDoFn extends DoFn<Feature,Feature>{
        @ProcessElement
        public void processElement(ProcessContext c) {
            if (predicate.test(c.element())) {
                c.output(c.element());
            }
        }
    }
}