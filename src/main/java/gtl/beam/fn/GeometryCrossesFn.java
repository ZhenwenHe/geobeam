package gtl.beam.fn;

import gtl.geom.Geometry;
import org.apache.beam.sdk.transforms.DoFn;

public class GeometryCrossesFn<T extends Geometry> extends DoFn<T,Boolean> {
    T geometry;

    public GeometryCrossesFn(T g) {
        this.geometry = g;
    }

    @ProcessElement
    public void processElement(ProcessContext c){
        Boolean b = this.geometry.crosses(c.element());
        c.output(b);
    }
}
