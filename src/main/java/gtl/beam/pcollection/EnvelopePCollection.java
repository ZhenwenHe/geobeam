package gtl.beam.pcollection;

import gtl.beam.pipeline.SpatialPipeline;
import gtl.beam.ptransform.EnvelopePTransforms;
import gtl.geom.Envelope;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class EnvelopePCollection  implements PValue, Serializable {
    private static final long serialVersionUID = 1L;

    private transient PCollection<Envelope> envelopes;

    public EnvelopePCollection(Pipeline p, List<Envelope> envelopes) {
        this.envelopes = p.apply(Create.<Envelope>of(envelopes));
    }

    public EnvelopePCollection(SpatialPipeline p, List<Envelope> envelopes) {
        this.envelopes = p.apply(Create.<Envelope>of(envelopes));
    }

    protected EnvelopePCollection(PCollection<Envelope> pc) {
        this.envelopes = pc;
    }

    public PCollection<Envelope> getEnvelopes() {
        return envelopes;
    }

    public Iterable<Envelope> extractEnvelopes() {
        final ArrayList<Envelope> al = new ArrayList<>();
        envelopes.apply(ParDo.of(new DoFn<Envelope, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c){
                al.add(c.element());
            }
        }));
        return al;
    }

    public EnvelopePCollection combine(){
        return new EnvelopePCollection(this.envelopes.apply(EnvelopePTransforms.combiner()));
    }

    @Override
    public String getName() {
        return this.envelopes.getName();
    }

    @Override
    public Pipeline getPipeline() {
        return this.envelopes.getPipeline();
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
        return this.envelopes.expand();
    }

    @Override
    public void finishSpecifyingOutput(String transformName, PInput input, PTransform<?, ?> transform) {
        this.envelopes.finishSpecifyingOutput(transformName,input,transform);
    }

    @Override
    public void finishSpecifying(PInput upstreamInput, PTransform<?, ?> upstreamTransform) {
        this.envelopes.finishSpecifying(upstreamInput,upstreamTransform);
    }

    /**
     * Like {@link PCollection.IsBounded#apply(String, PTransform)} but defaulting to the name
     * of the {@link PTransform}.
     *
     * @return the output of the applied {@link PTransform}
     */
    public <OutputT extends POutput> OutputT apply(PTransform<? super PCollection<Envelope>, OutputT> t) {
        return Pipeline.applyTransform(this.getEnvelopes(), t);
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
            String name, PTransform<? super PCollection<Envelope>, OutputT> t) {
        return Pipeline.applyTransform(name, this.getEnvelopes(), t);
    }


    public void print(){
        this.envelopes.apply(ParDo.of(new DoFn<Envelope, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c){
                Envelope pp = (Envelope) c.element();
                System.out.println(pp.toString());
            }
        }));
    }
}
