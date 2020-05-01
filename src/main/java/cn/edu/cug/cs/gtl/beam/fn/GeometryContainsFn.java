package cn.edu.cug.cs.gtl.beam.fn;

import cn.edu.cug.cs.gtl.geom.Geometry;
import org.apache.beam.sdk.transforms.DoFn;

public class GeometryContainsFn<T extends Geometry> extends DoFn<T,Boolean> {
    T geometry;

    public GeometryContainsFn(T g) {
        this.geometry = g;
    }

    @ProcessElement
    public void processElement(ProcessContext c){
        Boolean b = this.geometry.contains(c.element());
        c.output(b);
    }
}
