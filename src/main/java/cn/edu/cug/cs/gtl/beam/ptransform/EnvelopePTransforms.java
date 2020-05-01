package cn.edu.cug.cs.gtl.beam.ptransform;

import cn.edu.cug.cs.gtl.beam.fn.EnvelopeCombineFn;
import cn.edu.cug.cs.gtl.feature.Feature;
import cn.edu.cug.cs.gtl.geom.Envelope;
import cn.edu.cug.cs.gtl.geom.Geometry;
import cn.edu.cug.cs.gtl.io.FileDataSplitter;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;

public class EnvelopePTransforms {

    /**
     * 从要素集中提取每个要素的边界矩形
     * <code>
     *     PCollection<Envelope> pes = featureCollection.apply(EnvelopePTransforms.fromFeature());
     * </code>
     *
     * @param
     * @return
     */
    public static   MapElements<Feature,Envelope> fromFeature(){
        return MapElements.via(new SimpleFunction<Feature, Envelope>() {
            @Override
            public Envelope apply(Feature g){
                return g.getGeometry().getEnvelope();
            }
        });
    }

    /**
     * 从Geometry集中提取每个要素的边界矩形
     * <code>
     *     PCollection<Envelope> pes = geometryCollection.apply(EnvelopePTransforms.fromGeometry());
     * </code>
     *
     * @param
     * @return
     */
    public static   MapElements<Geometry,Envelope> fromGeometry(){
        return MapElements.via(new SimpleFunction<Geometry, Envelope>() {
            @Override
            public Envelope apply(Geometry g){
                return g.getEnvelope();
            }
        });
    }

    /**
     * 从要素集中提取每个要素的边界矩形,并合并成单个边界矩形，也就是计算要素集的最小边界矩形
     *  <code>
     *      PCollection<Envelope> pes=......
     *      PCollection<Envelope> pe = pes.apply(EnvelopePTransforms.combiner());
     *  </code>
     * @return
     */
    public static Combine.Globally<Envelope,Envelope> combiner(){
        return combiner(2);
    }

    public static Combine.Globally<Envelope,Envelope> combiner(final int dim){
        return Combine.globally(new EnvelopeCombineFn(dim));
    }

    /**
     *将Envelope对象转换成字符串，并打印出来
     * <code>
     *      PCollection<Envelope> pes = .......
     *      pes
     *          .apply(EnvelopePTransforms.printer( System.out))
     *          .setCoder(StringUtf8Coder.of())
     * </code>
     * @param
     * @return
     */
    public static ParDo.SingleOutput<Envelope, Void> printer(){
        return ParDo.of(new DoFn<Envelope, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c){
                System.out.println(c.element().toString());
            }
        });
    }

    /**
     * 从将Envelope转化成TSV字符串
     * <code>
     *     PCollection<Envelope> pes =......
     *     PCollection<String> pss = pes
     *     .apply(EnvelopePTransforms.toTSVString())
     *      .setCoder(StringUtf8Coder.of());
     * </code>
     *
     * @param
     * @return
     */
    public static   MapElements<Envelope,String> toTSVString(){
        return MapElements.via(new SimpleFunction<Envelope, String>() {
            @Override
            public String apply(Envelope g){
                return Envelope.toTSVString(g);
            }
        });
    }

    /**
     * 从TSV字符串转化成Envelope
     * <code>
     *     PCollection<String> pss =......
     *     PCollection<Envelope> pes =pss
     *     .apply(EnvelopePTransforms.fromTSVString())
     *      .setCoder(StorableCoder.of(Envelope.class));
     * </code>
     *
     * @param
     * @return
     */
    public static   MapElements<String,Envelope> fromTSVString(){
        return MapElements.via(new SimpleFunction<String,Envelope>() {
            @Override
            public Envelope apply( String g){
                return Envelope.fromTSVString(g);
            }
        });
    }

    /**
     * 计数器，统计PCollection中元素个数
     * <code>
     *     PCollection<Envelope> pfs =.......
     *     PCollection<Long> c= pfs.apply(FeaturePTransforms.counter());
     * </code>
     * @return
     */
    public static PTransform<PCollection<Envelope>, PCollection<Long>> counter(){
        return  Count.globally();
    }

    /**
     * 生成Envelope内的随机矩形
     * <code>
     *     PCollection<Envelope> pes =......
     *     PCollection<Envelope> pss = pes
     *     .apply(EnvelopePTransforms.random())
     *      .setCoder(StorableCoder.of(Envelope.class));
     * </code>
     *
     * @param
     * @return
     */
    public static   MapElements<Envelope,Envelope> random( final  double ratio){
        return MapElements.via(new SimpleFunction<Envelope, Envelope>() {
            @Override
            public Envelope apply(Envelope g){
                return Envelope.randomEnvelope(g,ratio);
            }
        });
    }


    /**
     * 从将Envelope转化成字符串
     * <code>
     *     PCollection<Envelope> pes =......
     *     PCollection<String> pss = pes
     *     .apply(EnvelopePTransforms.toByteString())
     *      .setCoder(StringUtf8Coder.of());
     * </code>
     *
     * @param
     * @return
     */
    public static   MapElements<Envelope,String> toByteString(){
        return MapElements.via(new SimpleFunction<Envelope, String>() {
            @Override
            public String apply(Envelope g){
                try {
                    return g.storeToByteString();
                }
                catch (IOException e){
                    e.printStackTrace();
                    return null;
                }
            }
        });
    }

    /**
     * 从字符串转化成Envelope
     * <code>
     *     PCollection<String> pss =......
     *     PCollection<Envelope> pes =pss
     *     .apply(EnvelopePTransforms.fromByteString())
     *      .setCoder(StorableCoder.of(Envelope.class));
     * </code>
     *
     * @param
     * @return
     */
    public static   MapElements<String,Envelope> fromByteString(){
        return MapElements.via(new SimpleFunction<String,Envelope>() {
            @Override
            public Envelope apply( String g){
                Envelope e = new Envelope();
                try {
                    e.loadFromByteString(g);
                    return e;
                }
                catch (IOException ex){
                    ex.printStackTrace();
                    return null;
                }
            }
        });
    }

}
