package gtl.beam.pvalue;

import gtl.geom.Envelope;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.PValueBase;
import org.apache.beam.sdk.values.TupleTag;

import java.util.Collections;
import java.util.Map;

@Deprecated
public class EnvelopePValue extends PValueBase  implements PValue,java.io.Serializable {
    private static final long serialVersionUID = 1L;
    private Envelope envelope=null;
    private final TupleTag<?> tag = new TupleTag<>();

    public EnvelopePValue(Pipeline pipeline, Envelope envelope) {
        super(pipeline);
        this.envelope = envelope;
    }

    public Envelope getEnvelope() {
        return envelope;
    }

    public void setEnvelope(Envelope envelope) {
        this.envelope = envelope;
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
        return Collections.singletonMap(tag, this);
    }

}
