package gtl.beam.pcollection;

import gtl.beam.pipeline.SpatialPipeline;
import gtl.common.Property;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.*;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class PropertyPCollection<T extends Property> implements PValue, Serializable {
    private static final long serialVersionUID = 1L;

    private transient PCollection<T> properties;

    public PropertyPCollection(Pipeline p, List<T> props) {
        this.properties=p.apply(Create.<T>of(props));
    }

    public PropertyPCollection(SpatialPipeline p, List<T> props) {
        this.properties=p.apply(Create.<T>of(props));
    }

    public PCollection<T> getProperties() {
        return properties;
    }

    @Override
    public String getName() {
        return this.properties.getName();
    }

    @Override
    public Pipeline getPipeline() {
        return this.properties.getPipeline();
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
        return this.properties.expand();
    }

    @Override
    public void finishSpecifyingOutput(String transformName, PInput input, PTransform<?, ?> transform) {
        this.properties.finishSpecifyingOutput(transformName,input,transform);
    }

    @Override
    public void finishSpecifying(PInput upstreamInput, PTransform<?, ?> upstreamTransform) {
        this.properties.finishSpecifying(upstreamInput,upstreamTransform);
    }

    /**
     * Like {@link PCollection.IsBounded#apply(String, PTransform)} but defaulting to the name
     * of the {@link PTransform}.
     *
     * @return the output of the applied {@link PTransform}
     */
    public <OutputT extends POutput> OutputT apply(PTransform<? super PCollection<T>, OutputT> t) {
        return Pipeline.applyTransform(this.properties, t);
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
        return Pipeline.applyTransform(name, this.properties, t);
    }

}
