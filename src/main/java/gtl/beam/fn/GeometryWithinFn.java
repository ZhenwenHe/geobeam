package gtl.beam.fn;

import gtl.geom.Geometry;
import org.apache.beam.sdk.transforms.DoFn;

public class GeometryWithinFn<T extends Geometry> extends DoFn<T,Boolean> {
    T geometry;

    public GeometryWithinFn(T g) {
        this.geometry = g;
    }

    @ProcessElement
    public void processElement(ProcessContext c){
        Boolean b = this.geometry.within(c.element());
        c.output(b);
    }
}
