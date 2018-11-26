package gtl.beam.pcollection;

import gtl.beam.pipeline.SpatialPipeline;
import gtl.geom.Vector;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.*;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class VectorPCollection <T extends Vector> implements PValue , Serializable {
    private static final long serialVersionUID = 1L;

    transient PCollection<T> vectors;

    public VectorPCollection(Pipeline p, List<T> vectors) {
        this.vectors = p.apply(Create.<T>of(vectors));
    }

    public VectorPCollection(SpatialPipeline p, List<T> vectors) {
        this.vectors = p.apply(Create.<T>of(vectors));
    }

    public PCollection<T> getVectors() {
        return vectors;
    }

    @Override
    public String getName() {
        return vectors.getName();
    }

    @Override
    public Pipeline getPipeline() {
        return vectors.getPipeline();
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
        return vectors.expand();
    }

    @Override
    public void finishSpecifyingOutput(String transformName, PInput input, PTransform<?, ?> transform) {
        vectors.finishSpecifyingOutput(transformName,input,transform);
    }

    @Override
    public void finishSpecifying(PInput upstreamInput, PTransform<?, ?> upstreamTransform) {
        vectors.finishSpecifying(upstreamInput,upstreamTransform);
    }

    /**
     * Like {@link PCollection.IsBounded#apply(String, PTransform)} but defaulting to the name
     * of the {@link PTransform}.
     *
     * @return the output of the applied {@link PTransform}
     */
    public <OutputT extends POutput> OutputT apply(PTransform<? super PCollection<T>, OutputT> t) {
        return Pipeline.applyTransform(this.getVectors(), t);
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
        return Pipeline.applyTransform(name, this.getVectors(), t);
    }
}
