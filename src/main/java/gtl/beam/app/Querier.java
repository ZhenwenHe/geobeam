package gtl.beam.app;

import gtl.beam.pipeline.SpatialPipeline;
import gtl.config.Config;
import gtl.data.sdb.beam.BeamDataSolution;
import gtl.data.sdb.beam.feature.BeamFeatureReader;
import gtl.data.sdb.beam.feature.BeamFeatureSet;
import gtl.data.sdb.beam.feature.BeamFeatureStore;
import gtl.feature.Feature;
import gtl.filter.EnvelopeContainsFeatureFilter;
import gtl.filter.FeatureFilter;
import gtl.geom.Envelope;
import gtl.io.File;
import org.apache.beam.sdk.options.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 输入矩形边界，进行查询
 * mvn package exec:java -Dexec.mainClass=gtl.beam.app.Querier
 * -Dexec.args="
 * --inputFile=/home/vincent/gtl/data/dat/counties/county_small.tsv
 * --outputFile=/home/vincent/gtl/data/dat/temp/county_small.querier
 * --minX=-130.0
 * --minY=-100.0
 * --minZ=0.0
 * --maxX=30.0
 * --maxY=50.0
 * --maxZ==0.0
 * --runner=FlinkRunner"
 * -Pflink-runner
 * 上面的命令运行有BUG，Z 值采用默认的就可以
 * mvn package exec:java -Dexec.mainClass=gtl.beam.app.Querier -Dexec.args=" --inputFile=/home/vincent/gtl/data/dat/counties/county_small.tsv --outputFile=/home/vincent/gtl/data/dat/temp/county_small.analyzer --minX=-130.0 --minY=-100.0 --maxX=30.0 --maxY=50.0 --runner=FlinkRunner" -Pflink-runner
 * $FLINK_HOME/bin/flink run -c gtl.beam.app.Querier ./target/geobeam-bundled-1.0-SNAPSHOT.jar  --inputFile=/home/vincent/gtl/data/dat/counties/county_small.tsv --outputFile=/home/vincent/gtl/data/dat/temp/county_small.querier --minX=-130.0 --minY=-100.0 --maxX=30.0 --maxY=50.0 --runner=FlinkRunner
 */
public class Querier {

    protected static final Logger LOG = LoggerFactory.getLogger(Querier.class);

    public static void main(String [] args){

        if(args==null ||  args.length<2){
            String[] defaultArgs = new String[9];
            defaultArgs[0]="--inputFile="+ Config.getDataDirectory()+ File.separator+"counties"+File.separator+"county_small.tsv";
            defaultArgs[1]="--outputFile="+Config.getTemporaryDirectory()+File.separator+"county_small.querier";
            defaultArgs[2]="--minX=-130";
            defaultArgs[3]="--minY=-100";
            defaultArgs[4]="--maxX=30";
            defaultArgs[5]="--maxY=50";
            defaultArgs[6]="--minZ=0";
            defaultArgs[7]="--maxZ=0";
            defaultArgs[8]="--runner=DirectRunner";
            args=defaultArgs;
        }

        QuerierPipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(QuerierPipelineOptions.class);

        SpatialPipeline p = new SpatialPipeline(options);

        final String inputDataFile=options.getInputFile();
        final String outputDataFile=options.getOutputFile();
        final double x1=options.getMinX();
        final double y1=options.getMinY();
        final double z1=options.getMinZ();
        final double x2=options.getMaxX();
        final double y2=options.getMaxY();
        final double z2=options.getMaxZ();
        Envelope e = null;
        if(z1==z2){
            e = new Envelope(x1,x2,y1,y2);
        }
        else {
            e=new Envelope(x1,x2,y1,y2,z1,z2);
        }
        final FeatureFilter featureFilter = (FeatureFilter) new EnvelopeContainsFeatureFilter(e);
        BeamDataSolution solution = new BeamDataSolution(p.getPipeline());

        try {
            BeamFeatureStore fs = (BeamFeatureStore) solution.createFeatureStore("example");
            BeamFeatureSet bfs = (BeamFeatureSet) fs.createFeatureSetFromTSVFile(p.getPipeline(),inputDataFile);
            BeamFeatureReader reader = (BeamFeatureReader) bfs.getDataReader(featureFilter);
            reader.writeTSV(outputDataFile);
            p.getPipeline().run().waitUntilFinish();
        }
        catch (IOException ex){
            ex.printStackTrace();
        }
        catch (InterruptedException ex){
            ex.printStackTrace();
        }
    }

    public interface QuerierPipelineOptions extends PipelineOptions {

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

        @Description("max x")
        @Default.Double(0)
        @Validation.Required
        Double getMaxX();
        void setMaxX(Double value);

        @Description("max y")
        @Default.Double(0)
        @Validation.Required
        Double getMaxY();
        void setMaxY(Double value);

        @Description("max z")
        @Default.Double(0)
        @Validation.Required
        Double getMaxZ();
        void setMaxZ(Double value);

        @Description("min x")
        @Default.Double(0)
        @Validation.Required
        Double getMinX();
        void setMinX(Double value);

        @Description("min y")
        @Default.Double(0)
        @Validation.Required
        Double getMinY();
        void setMinY(Double value);

        @Description("min z")
        @Default.Double(0)
        @Validation.Required
        Double getMinZ();
        void setMinZ(Double value);
    }


}
