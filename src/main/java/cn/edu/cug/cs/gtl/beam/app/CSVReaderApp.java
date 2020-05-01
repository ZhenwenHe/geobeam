package cn.edu.cug.cs.gtl.beam.app;

import cn.edu.cug.cs.gtl.beam.pipeline.SpatialPipeline;
import cn.edu.cug.cs.gtl.beam.ptransform.StringPTransforms;
import cn.edu.cug.cs.gtl.config.Config;
import cn.edu.cug.cs.gtl.io.File;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Sample;

/**
 * @author YaboSun
 *
 * 将深圳监测数据读取 采样输出
 */
public class CSVReaderApp {

    public static void main(String [] args) {

        if(args == null || args.length < 2) {
            String[] defaultArgs = new String[4];
            defaultArgs[0] = "--inputFile="
                    + Config.getDataDirectory() + File.separator
                    + "monitoring" + File.separator
                    + "rainfall" + File.separator
                    + "szytdz-0004-StatisticsData_20170701-31(07).csv";
            defaultArgs[1] = "--outputFile="
                    + Config.getTemporaryDirectory() + File.separator
                    + "szytdz-0004-StatisticsData_20170701-31(07).sampler";
            defaultArgs[2] = "--readNumber=1";
            defaultArgs[3] = "--runner=DirectRunner";
            args = defaultArgs;
        }

        CSVReaderApp.CSVReaderPipelineOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(CSVReaderApp.CSVReaderPipelineOptions.class);

        SpatialPipeline pipeline = new SpatialPipeline(options);

        final String inputDataFile = options.getInputFile();
        final String outputDataFile = options.getOutputFile();
        final Long sm = options.getReadNumber();

        pipeline.getPipeline()
                .apply(TextIO.read().from(inputDataFile))
                .setCoder(StringUtf8Coder.of())
                .apply(Sample.any(sm))
                .apply(StringPTransforms.writer(outputDataFile));
        pipeline.getPipeline()
                .run()
                .waitUntilFinish();
    }

    public interface CSVReaderPipelineOptions extends PipelineOptions {

        @Description("Path of the file to read from")
        @Default.String("hdfs://geosciences/data/spatialhadoop/monitoring/rainfall/szytdz-0004-StatisticsData_20170701-31(07).csv")
        @Validation.Required
        String getInputFile();
        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Default.String("hdfs://geosciences/data/spatialhadoop/monitoring/rainfall")
        String getOutputFile();
        void setOutputFile(String value);

        @Description("read number")
        @Default.Long(5)
        @Validation.Required
        Long getReadNumber();
        void setReadNumber(Long value);
    }
}
