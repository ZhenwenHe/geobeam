package gtl.beam.pcollection;

import gtl.beam.pipeline.SpatialPipeline;
import gtl.feature.FeatureType;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.*;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class FeatureTypePCollection<T extends FeatureType> implements PValue, Serializable {
    private static final long serialVersionUID = 1L;

    transient PCollection<T> featureTypes;

    public FeatureTypePCollection(Pipeline p, List<T> featureTypes) {
        this.featureTypes = p.apply(Create.<T>of(featureTypes));
    }

    public FeatureTypePCollection(SpatialPipeline p, List<T> featureTypes) {
        this.featureTypes = p.apply(Create.<T>of(featureTypes));
    }


    public PCollection<T> getFeatureTypes() {
        return featureTypes;
    }

    @Override
    public String getName() {
        return this.featureTypes.getName();
    }

    @Override
    public Pipeline getPipeline() {
        return this.featureTypes.getPipeline();
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
        return this.featureTypes.expand();
    }

    @Override
    public void finishSpecifyingOutput(String transformName, PInput input, PTransform<?, ?> transform) {
        this.featureTypes.finishSpecifyingOutput(transformName,input,transform);
    }

    @Override
    public void finishSpecifying(PInput upstreamInput, PTransform<?, ?> upstreamTransform) {
        this.featureTypes.finishSpecifying(upstreamInput,upstreamTransform);
    }

    /**
     * Like {@link PCollection.IsBounded#apply(String, PTransform)} but defaulting to the name
     * of the {@link PTransform}.
     *
     * @return the output of the applied {@link PTransform}
     */
    public <OutputT extends POutput> OutputT apply(PTransform<? super PCollection<T>, OutputT> t) {
        return Pipeline.applyTransform(this.featureTypes, t);
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
        return Pipeline.applyTransform(name, this.featureTypes, t);
    }
}
