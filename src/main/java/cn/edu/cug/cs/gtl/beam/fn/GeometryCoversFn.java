package cn.edu.cug.cs.gtl.beam.fn;

import cn.edu.cug.cs.gtl.geom.Geometry;
import org.apache.beam.sdk.transforms.DoFn;

public class GeometryCoversFn<T extends Geometry> extends DoFn<T,Boolean> {
    T geometry;

    public GeometryCoversFn(T g) {
        this.geometry = g;
    }

    @ProcessElement
    public void processElement(ProcessContext c){
        Boolean b = this.geometry.covers(c.element());
        c.output(b);
    }
}
