package cn.edu.cug.cs.gtl.beam.ptransform;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class FromIterablePTransform<T> extends PTransform<PCollection<Iterable<T>> ,PCollection<T> > {
    @Override
    public PCollection<T> expand(PCollection<Iterable<T>> input) {
        PCollection<T> pc = input.apply(ParDo.of(new DoFn<Iterable<T>, T>() {
            @ProcessElement
            public void processElement(ProcessContext c){
                for(T t : c.element())
                    c.output(t);
            }
        }));
        return pc;
    }

    public static <T> FromIterablePTransform<T> of(){
        return new FromIterablePTransform<T>();
    }
}
