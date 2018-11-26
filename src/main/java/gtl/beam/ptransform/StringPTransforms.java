package gtl.beam.ptransform;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class StringPTransforms  {

    /**
     * String打印出来
     * <code>
     *     PCollection<String> pfs = .......
     *     pfs
     *     .apply(StringPTransforms.printer())
     *     .setCoder(StringUtf8Coder.of())
     * </code>
     * @return
     */
    public static ParDo.SingleOutput<String, Void> printer(){
        return ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c){
                System.out.println(c.element());
            }
        });
    }


    /**
     * 读取文件中的字符串，存储为字符串PCollection
     * <code>
     *     Pipeline p;
     *     ....//create pipeline;
     *     PCollection<String> csvLines = p
     *     .apply( StringPTransforms.reader(inputDataFile))
     *     .setCoder(StringUtf8Coder.of());
     * </code>
     * @param inputDataFile
     * @return
     */
    public static  TextIO.Read reader(String inputDataFile){
        return TextIO.read().from(inputDataFile);
    }

    /**
     * 将字符串PCollection写出到文本文件
     * <code>
     *     Pipeline p;
     *     ....//create pipeline;
     *     PCollection<String> csvLines = ......
     *     csvLines.apply( StringPTransforms.writer(outDataFile))
     *     .setCoder(StringUtf8Coder.of());
     * </code>
     * @param outDataFile
     * @return
     */
    public static  TextIO.Write writer(String outDataFile){
        return TextIO.write().to(outDataFile).withoutSharding();
    }

    /**
     * 计数器，统计PCollection中元素个数
     * <code>
     *     PCollection<String> pfs =.......
     *     PCollection<Long> c= pfs.apply(FeaturePTransforms.counter());
     * </code>
     * @return
     */
    public static PTransform<PCollection<String>, PCollection<Long>> counter(){
        return  Count.globally();
    }
}
