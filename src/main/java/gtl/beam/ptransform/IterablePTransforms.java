package gtl.beam.ptransform;

import gtl.beam.fn.ToIterableFn;
import org.apache.beam.sdk.transforms.Combine;

public class IterablePTransforms {


    /**
     * 将PCollection<T>转换成PCollection<Iterable<T>>,例如
     * <code>
     *     PCollection<Feature> pfs = ......
     *     PCollection<Iterable<Feature>> pis =
     *          pfs.apply(IterablePTransforms.<Feature>toIterable());
     * </code>
     * @param <T>
     * @return
     */
    public static <T> Combine.Globally<T, Iterable<T>> toIterable(){
        return Combine.globally(new ToIterableFn<T>());
    }

    /**
     * 将PCollection<Iterable<T>>转换成PCollection<T>,例如
     * <code>
     *     PCollection<Feature> pfs = ......
     *     PCollection<Iterable<Feature>> pis =
     *          pfs.apply(IterablePTransforms.<Feature>toIterable());
     *     PCollection<Feature> pfs2= pis.apply(IterablePTransforms.<Feature>fromIterbale());
     *     assertEquals(pfs,pfs2);
     * </code>
     * @param <T>
     * @return
     */
    public static <T> FromIterablePTransform<T> fromIterbale(){
        return FromIterablePTransform.<T>of();
    }
}
