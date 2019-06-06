package gtl.beam.app;

import gtl.beam.pipeline.SpatialPipeline;
import gtl.beam.ptransform.StringPTransforms;
import gtl.config.Config;
import gtl.io.File;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Sample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 采样输出，便于查看大数据集合的格式
 */
public class Sampler {

    protected static final Logger LOG = LoggerFactory.getLogger(Sampler.class);

    public static void main(String [] args){

        if(args==null ||  args.length<2){
            String[] defaultArgs = new String[4];
            defaultArgs[0]="--inputFile="+ Config.getDataDirectory()+ File.separator+"counties"+File.separator+"county_small.tsv";
            defaultArgs[1]="--outputFile="+Config.getTemporaryDirectory()+File.separator+"county_small.sampler";
            defaultArgs[2]="--sampleNumber=5";
            defaultArgs[3]="--runner=DirectRunner";
            args=defaultArgs;
        }

        SamplerPipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(SamplerPipelineOptions.class);

        SpatialPipeline p = new SpatialPipeline(options);

        final String inputDataFile=options.getInputFile();
        final String outputDataFile=options.getOutputFile();
        final Long sm = options.getSampleNumber();

        p.getPipeline()
                .apply(TextIO.read().from(inputDataFile))
                .setCoder(StringUtf8Coder.of())
                .apply(Sample.any(sm))
                .apply(StringPTransforms.writer(outputDataFile));
        p.getPipeline().run().waitUntilFinish();
    }

    public interface SamplerPipelineOptions extends PipelineOptions {

        @Description("Path of the file to read from")
        /*@Default.String("hdfs://geosciences/data/spatialhadoop/counties/county_small.tsv")*/
        @Default.String("hdfs://geosciences/data/spatialhadoop/counties/county_small.tsv")
        @Validation.Required
        String getInputFile();
        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Default.String("hdfs://geosciences/data/spatialhadoop/counties/county_small")
        String getOutputFile();
        void setOutputFile(String value);

        @Description("sample number")
        @Default.Long(5)
        @Validation.Required
        Long getSampleNumber();
        void setSampleNumber(Long value);
    }

}
