package cn.edu.cug.cs.gtl.beam.fn;

import cn.edu.cug.cs.gtl.geom.Envelope;
import cn.edu.cug.cs.gtl.io.Storable;
import org.apache.beam.sdk.transforms.Combine;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 */
public class EnvelopeCombineFn
        extends Combine.CombineFn<Envelope,EnvelopeCombineFn.Accumulator,Envelope> {
    private static final long serialVersionUID = 1L;
    final int shapeDimension;

    public EnvelopeCombineFn(int shapeDimension) {
        this.shapeDimension=shapeDimension;
    }

    public EnvelopeCombineFn( ) {
        this.shapeDimension=2;
    }
    @Override
    public Accumulator createAccumulator() {
        return new Accumulator(this.shapeDimension);
    }

    @Override
    public Accumulator addInput(Accumulator accumulator, Envelope input) {
        accumulator.totalEnvelope.combine(input);
        return accumulator;
    }

    @Override
    public Accumulator mergeAccumulators(Iterable<Accumulator> accumulators) {
        Accumulator mergeAccumulator = createAccumulator();
        for(Accumulator a : accumulators){
            mergeAccumulator.totalEnvelope.combine(a.totalEnvelope);
        }
        return mergeAccumulator;
    }

    @Override
    public Envelope extractOutput(Accumulator accumulator) {
        return accumulator.totalEnvelope;
    }

    public static class Accumulator implements Storable {
        public Envelope totalEnvelope;

        public Accumulator(Envelope totalEnvelope) {
            this.totalEnvelope = (Envelope) totalEnvelope.clone();
        }

        public Accumulator( int shapeDimension) {
            this.totalEnvelope = new Envelope(shapeDimension);
        }
        public Accumulator(  ) {
            this.totalEnvelope = new Envelope(2);
        }
        @Override
        public Object clone() {
            return new Accumulator(this.totalEnvelope);
        }

        @Override
        public void copyFrom(Object i) {
            if(i instanceof  Accumulator){
                this.totalEnvelope = (Envelope) ((Accumulator)i).totalEnvelope.clone();
            }
        }

        @Override
        public boolean load(DataInput in) throws IOException {
            return this.totalEnvelope.load(in);
        }

        @Override
        public boolean store(DataOutput out) throws IOException {
            return this.totalEnvelope.store(out);
        }

        @Override
        public long getByteArraySize() {
            return this.totalEnvelope.getByteArraySize();
        }
    }

}
