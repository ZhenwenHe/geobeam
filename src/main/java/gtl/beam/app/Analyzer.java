package gtl.beam.app;

import gtl.beam.coder.StorableCoder;
import gtl.beam.pipeline.SpatialPipeline;
import gtl.beam.ptransform.EnvelopePTransforms;
import gtl.beam.ptransform.FeaturePTransforms;
import gtl.beam.ptransform.FeatureTypePTransforms;
import gtl.beam.ptransform.StringPTransforms;
import gtl.config.Config;
import gtl.data.sdb.beam.BeamDataSolution;
import gtl.data.sdb.beam.feature.BeamFeatureSet;
import gtl.data.sdb.beam.feature.BeamFeatureStore;
import gtl.feature.Feature;
import gtl.feature.FeatureType;
import gtl.geom.Envelope;
import gtl.geom.Geometry;
import gtl.io.File;
import gtl.io.FileDataSplitter;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 分析静态数据集的边界、要素个数，并采样，采样结果采用TSV格式输出
 * mvn package exec:java -Dexec.mainClass=gtl.beam.app.Analyzer
 * -Dexec.args="--inputFile=/home/vincent/gtl/data/dat/counties/county_small.tsv
 * --outputFile=/home/vincent/gtl/data/dat/temp/county_small.analyzer --runner=FlinkRunner"
 * -Pflink-runner
 */
public class Analyzer {

    private static final Logger LOG = LoggerFactory.getLogger(Analyzer.class);
    public static void main(String [] args){

        if(args==null ||  args.length<2){
            String[] defaultArgs = new String[4];
            defaultArgs[0]="--inputFile="+ Config.getDataDirectory()+ File.separator+"counties"+File.separator+"county_small.tsv";
            defaultArgs[1]="--outputFile="+Config.getTemporaryDirectory()+File.separator+"county_small.analyzer";
            defaultArgs[2]="--sampleNumber=10";
            defaultArgs[3]="--runner=DirectRunner";
            args=defaultArgs;
        }

        AnalyzerOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(AnalyzerOptions.class);

        SpatialPipeline p = new SpatialPipeline(options);

        final String inputDataFile=options.getInputFile();
        final String inputSchemaFile = inputDataFile+".schema";//schema file ,which is used to create feature type
        final String outputDataFile=options.getOutputFile();
        final  Long sampleNumber =options.getSampleNumber();

        BeamDataSolution solution = new BeamDataSolution(p.getPipeline());

        try {
            BeamFeatureStore fs = (BeamFeatureStore) solution.createFeatureStore("example");
            BeamFeatureSet bfs = null;
            String suffix = File.getSuffixName(inputDataFile);
            if(suffix.equalsIgnoreCase("TSV"))
                bfs=(BeamFeatureSet) fs.createFeatureSetFromTSVFile(p.getPipeline(),inputDataFile);
            else if(suffix.equalsIgnoreCase("SSV"))
                bfs=(BeamFeatureSet) fs.createFeatureSetFromSSVFile(p.getPipeline(),inputDataFile);
            else
                bfs=(BeamFeatureSet) fs.createFeatureSetFromCSVFile(p.getPipeline(),inputDataFile);
            //dataset boundary
            PCollection<String> ps1 = bfs
                    .calculateEnvelope()
                    .apply(EnvelopePTransforms.toTSVString())
                    .setCoder(StringUtf8Coder.of());
            //dataset count
            PCollection<String> ps2 = bfs
                    .calculateCounter()
                    .apply(MapElements.via(new SimpleFunction<Long, String>() {
                        @Override
                        public String apply(Long l){
                            return l.toString();
                        }
                    }))
                    .setCoder(StringUtf8Coder.of());
            //sampling
            PCollection<Feature> pf = bfs.anySampling(sampleNumber.longValue());
            PCollection<String> ps3 = pf
                    .apply(FeaturePTransforms.tsvString())
                    .setCoder(StringUtf8Coder.of());
            //convert to strings
            PCollectionList<String> ps = PCollectionList.of(ps1).and(ps2).and(ps3);
            PCollection<String> mergedCollectionWithFlatten = ps.apply(Flatten.<String>pCollections());
            mergedCollectionWithFlatten.apply(StringPTransforms.writer(outputDataFile));
            p.getPipeline().run().waitUntilFinish();
        }
        catch (IOException e){
            e.printStackTrace();
        }
        catch (InterruptedException e){
            e.printStackTrace();
        }
    }

    public interface AnalyzerOptions extends PipelineOptions {

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
        @Default.Long(100)
        @Validation.Required
        Long getSampleNumber();
        void setSampleNumber(Long value);
    }

}
