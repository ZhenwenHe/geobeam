package gtl.beam.pcollection;

import gtl.beam.pipeline.SpatialPipeline;
import gtl.common.Variant;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.*;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class VariantPCollection <T extends Variant> implements PValue , Serializable {
    private static final long serialVersionUID = 1L;

    private transient PCollection<T> values;

    public VariantPCollection(Pipeline p, List<T> values) {
        this.values = p.apply(Create.<T>of(values));
    }

    public VariantPCollection(SpatialPipeline p, List<T> values) {
        this.values = p.apply(Create.<T>of(values));
    }

    public PCollection<T> getValues() {
        return values;
    }

    @Override
    public String getName() {
        return this.getValues().getName();
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
        return this.getValues().expand();
    }

    @Override
    public void finishSpecifying(PInput upstreamInput, PTransform<?, ?> upstreamTransform) {
        this.getValues().finishSpecifying(upstreamInput,upstreamTransform);
    }

    @Override
    public Pipeline getPipeline() {
        return this.values.getPipeline();
    }

    @Override
    public void finishSpecifyingOutput(String transformName, PInput input, PTransform<?, ?> transform) {
        this.getValues().finishSpecifyingOutput(transformName,input,transform);
    }

    /**
     * Like {@link PCollection.IsBounded#apply(String, PTransform)} but defaulting to the name
     * of the {@link PTransform}.
     *
     * @return the output of the applied {@link PTransform}
     */
    public <OutputT extends POutput> OutputT apply(PTransform<? super PCollection<T>, OutputT> t) {
        return Pipeline.applyTransform(this.values, t);
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
        return Pipeline.applyTransform(name, this.values, t);
    }

}
