package cn.edu.cug.cs.gtl.beam.pipeline;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


public class SpatialPipeline implements Serializable {
    private static final long serialVersionUID = 1L;
    private final transient Pipeline pipeline;

    public SpatialPipeline(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    public SpatialPipeline(String[] args) {
        PipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).create();
        pipeline = Pipeline.create(options);
    }

    public SpatialPipeline(PipelineOptions options) {
        pipeline = Pipeline.create(options);
    }

    public Pipeline getPipeline() {
        return pipeline;
    }


    public <OutputT extends POutput> OutputT apply(
            PTransform<? super PBegin, OutputT> root) {
        return this.pipeline.<OutputT>apply(root);
    }

    public <OutputT extends POutput> OutputT apply(
            String name, PTransform<? super PBegin, OutputT> root) {
        return this.pipeline.<OutputT>apply(name, root);
    }

    public PipelineResult run() {
        return this.pipeline.run();
    }

    /**
     * Runs this {@link Pipeline} using the given {@link PipelineOptions}, using the runner specified
     * by the options.
     */
    public PipelineResult run(PipelineOptions options) {
       return this.run(options);
    }


    public CoderRegistry getCoderRegistry() {
        return this.pipeline.getCoderRegistry();
    }


    public static <T> List<T> collect(PCollection<T> p){
        ArrayList<T> arrayList = new ArrayList<>();
        return arrayList;
    }
}


