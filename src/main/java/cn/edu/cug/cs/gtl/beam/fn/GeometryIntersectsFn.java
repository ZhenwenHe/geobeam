package cn.edu.cug.cs.gtl.beam.fn;

import cn.edu.cug.cs.gtl.geom.Geometry;
import org.apache.beam.sdk.transforms.DoFn;

public class GeometryIntersectsFn<T extends Geometry> extends DoFn<T,Boolean> {
    T geometry;

    public GeometryIntersectsFn(T g) {
        this.geometry = g;
    }

    @ProcessElement
    public void processElement(ProcessContext c){
        Boolean b = this.geometry.intersects(c.element());
        c.output(b);
    }
}
