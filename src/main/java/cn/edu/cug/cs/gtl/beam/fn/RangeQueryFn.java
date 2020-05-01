package cn.edu.cug.cs.gtl.beam.fn;

import cn.edu.cug.cs.gtl.geom.Envelope;
import cn.edu.cug.cs.gtl.geom.Geometry;
import org.apache.beam.sdk.transforms.DoFn;

public class RangeQueryFn <E extends Envelope, T extends Geometry> extends DoFn<T,T> {
    E geometry;

    public RangeQueryFn(E g) {
        this.geometry = g;
    }

    @DoFn.ProcessElement
    public void processElement(ProcessContext c){
        T g = c.element();
        Envelope e = g.getEnvelope();
        if(this.geometry.intersects(e))
            c.output(g);
    }
}
