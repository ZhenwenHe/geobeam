package gtl.beam.pcollection;

import gtl.beam.pipeline.SpatialPipeline;
import gtl.feature.Feature;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.*;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class FeaturePCollection <T extends Feature> implements PValue, Serializable {
    private static final long serialVersionUID = 1L;

    private transient PCollection<T> features;

    public FeaturePCollection(Pipeline p, List<T> featureList) {
        this.features = p.apply(Create.<T>of(featureList));
    }

    public FeaturePCollection(SpatialPipeline p, List<T> featureList) {
        this.features = p.apply(Create.<T>of(featureList));
    }

    public PCollection<T> getFeatures() {
        return features;
    }

    @Override
    public String getName() {
        return this.features.getName();
    }

    @Override
    public Pipeline getPipeline() {
        return this.features.getPipeline();
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
        return this.features.expand();
    }

    @Override
    public void finishSpecifyingOutput(String transformName, PInput input, PTransform<?, ?> transform) {
        this.features.finishSpecifyingOutput(transformName,input,transform);
    }

    @Override
    public void finishSpecifying(PInput upstreamInput, PTransform<?, ?> upstreamTransform) {
        this.features.finishSpecifying(upstreamInput,upstreamTransform);
    }

    /**
     * Like {@link PCollection.IsBounded#apply(String, PTransform)} but defaulting to the name
     * of the {@link PTransform}.
     *
     * @return the output of the applied {@link PTransform}
     */
    public <OutputT extends POutput> OutputT apply(PTransform<? super PCollection<T>, OutputT> t) {
        return Pipeline.applyTransform(this.features, t);
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
        return Pipeline.applyTransform(name, this.features, t);
    }
}
