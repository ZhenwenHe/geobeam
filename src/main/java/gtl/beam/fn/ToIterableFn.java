package gtl.beam.fn;

import org.apache.beam.sdk.transforms.Combine;

import java.util.ArrayList;
import java.util.List;

public class ToIterableFn<T extends Object>
        extends Combine.CombineFn<T,List<T>,Iterable<T>> {
    private static final long serialVersionUID = 1L;

    @Override
    public List<T> createAccumulator() {
        ArrayList<T> al = new ArrayList<>();
        return al;
    }

    @Override
    public List<T> addInput(List<T> accumulator, T input) {
        accumulator.add(input);
        return accumulator;
    }

    @Override
    public List<T> mergeAccumulators(Iterable<List<T>> accumulators) {
        List<T> al = createAccumulator();
        for(List<T> l: accumulators)
            al.addAll(l);
        return al;
    }

    @Override
    public Iterable<T> extractOutput(List<T> accumulator) {
        return accumulator;
    }
}
