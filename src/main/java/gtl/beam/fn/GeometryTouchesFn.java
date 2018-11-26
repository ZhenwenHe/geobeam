package gtl.beam.fn;

import gtl.geom.Geometry;
import org.apache.beam.sdk.transforms.DoFn;

public class GeometryTouchesFn<T extends Geometry> extends DoFn<T,Boolean> {
    T geometry;

    public GeometryTouchesFn(T g) {
        this.geometry = g;
    }

    @ProcessElement
    public void processElement(ProcessContext c){
        Boolean b = this.geometry.touches(c.element());
        c.output(b);
    }
}
