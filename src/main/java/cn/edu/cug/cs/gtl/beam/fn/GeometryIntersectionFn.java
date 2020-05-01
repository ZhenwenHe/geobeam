package cn.edu.cug.cs.gtl.beam.fn;

import cn.edu.cug.cs.gtl.geom.Geometry;
import org.apache.beam.sdk.transforms.DoFn;

public class GeometryIntersectionFn<T extends Geometry> extends DoFn<T,Geometry> {
    T geometry;

    public GeometryIntersectionFn(T g) {
        this.geometry = g;
    }

    @ProcessElement
    public void processElement(ProcessContext c){
        Geometry g = this.geometry.intersection(c.element());
        c.output(g);
    }
}
