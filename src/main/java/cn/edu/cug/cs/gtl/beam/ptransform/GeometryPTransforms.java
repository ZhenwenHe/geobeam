package cn.edu.cug.cs.gtl.beam.ptransform;


import cn.edu.cug.cs.gtl.geom.Geometry;
import cn.edu.cug.cs.gtl.io.wkt.WKTReader;
import cn.edu.cug.cs.gtl.io.wkt.WKTWriter;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;

/**
 * <code>
 *     p.apply(GeometryPTransforms.tsvReader(inputDataFile))
 *                 .setCoder(StringUtf8Coder.of())
 *                 .apply(GeometryPTransforms.tsvFilter(geomType))
 *                 .setCoder(StringUtf8Coder.of())
 *                 .apply(GeometryPTransforms.parser(FileDataSplitter.TSV.getDelimiter()))
 *                 .setCoder(GeometryCoder.of(Geometry.class))
 *                 .apply(GeometryPTransforms.wktString())
 *                 .setCoder(StringUtf8Coder.of())
 *                 .apply(StringPTransforms.writer(outputDataFile+"2.wkt"));
 * </code>
 */
public  class GeometryPTransforms {

    /**
     * 从将Geometry转化成WKT字符串
     * 目前只支持2D，主要是WKTWriterImpl只支持2D，还需要扩展3D
     * <code>
     *     PCollection<Geometry> pes =......
     *     PCollection<String> pss = pes
     *     .apply(GeometryPTransforms.tsvString())
     *      .setCoder(StringUtf8Coder.of());
     * </code>
     *
     * @param
     * @return
     */
    public static MapElements<Geometry,String> wktString(){
        return MapElements.via(new SimpleFunction<Geometry, String>() {
            @Override
            public String apply(Geometry g){
                return WKTWriter.create(2).write(g);
            }
        });
    }

    /**
     * 读取TSV文件,存储为字符串PCollection
     * <code>
     *     Pipeline p;
     *     ....//create pipeline;
     *     PCollection<String> tsvLines = p
     *     .apply( FeaturePTransforms.tsvReader(inputDataFile))
     *     .setCoder(StringUtf8Coder.of());
     * </code>
     * @param inputDataFile
     * @return
     */
    public static  TextIO.Read tsvReader(String inputDataFile){
        return TextIO.read().from(inputDataFile);
    }

    /**
     * 读取CSV文件,存储为字符串PCollection
     * <code>
     *     Pipeline p;
     *     ....//create pipeline;
     *     PCollection<String> csvLines = p
     *     .apply( GeometryPTransforms.csvReader(inputDataFile))
     *     .setCoder(StringUtf8Coder.of());
     * </code>
     * @param inputDataFile
     * @return
     */
    public static  TextIO.Read csvReader(String inputDataFile){
        return TextIO.read().from(inputDataFile);
    }

    /**
     * 从TSV字符串集合中提取指定集合类型的字符串
     * <code>
     *     PCollection<String> tsvLines4GeomType = tsvLines
     *     .apply( GeometryPTransforms.tsvFilter(Geometry.POLYGON))
     *     .setCoder(StringUtf8Coder.of())
     * </code>
     * @param geomType
     * @return
     */
    public static  org.apache.beam.sdk.transforms.Filter<String> tsvFilter(int geomType){
        return org.apache.beam.sdk.transforms.Filter.by(new SerializableFunction<String,Boolean>() {
            @Override
            public Boolean apply(String line){
                int i = line.indexOf('(');
                String tag = line.substring(0,i);
                return tag.trim().equalsIgnoreCase(Geometry.getSimpleTypeName(geomType)) ;
            }
        });
    }

    /**
     * 从CSV字符串集合中提取指定集合类型的字符串
     * <code>
     *     PCollection<String> tsvLines4GeomType = csvLines
     *     .apply( GeometryPTransforms.csvFilter(Geometry.POLYGON))
     *     .setCoder(StringUtf8Coder.of())
     * </code>
     * @param geomType
     * @return
     */
    public static  org.apache.beam.sdk.transforms.Filter<String> csvFilter(int geomType){
        return org.apache.beam.sdk.transforms.Filter.by(new SerializableFunction<String,Boolean>() {
            @Override
            public Boolean apply(String line){
                int i = line.indexOf('(');
                String tag = line.substring(0,i);
                return tag.trim().equalsIgnoreCase(Geometry.getSimpleTypeName(geomType)) ;
            }
        });
    }

    /**
     * 从字符串中解析信息构建Feature，例如，从TSV字符串中解析Feature
     * <code>
     *     PCollection<String> tsvLines=.....
     *     final FeatureType featureType=......
     *     PCollection<Geometry> pfs = tsvLine
     *      .apply( GeometryPTransforms.parser(FileDataSplitter.TSV.getDelimiter()))
     *      .setCoder(GeometryCoder.of(Geometry.class));
     * </code>
     * @param delimiter 字符串的分割符，
     *                  例如FileDataSplitter.TSV.getDelimiter()
     *                  FileDataSplitter.CSV.getDelimiter()等
     * @return
     */
    public static MapElements<String, Geometry> parser(final String delimiter){
        return MapElements.via(new SimpleFunction<String, Geometry>() {
            @Override
            public Geometry apply(String line){
                String[] columns = line.split(delimiter);
                return WKTReader.create().read(columns[0]);
            }
        });
    }

    /**
     * 将Feature中几何对象转换成WKT字符串，并打印出来
     * 目前只支持2D，主要是WKTWriterImpl只支持2D，还需要扩展3D
     * <code>
     *     PCollection<Feature> pfs = .......
     *     pfs
     *     .apply(GeometryPTransforms.wktPrinter())
     *     .setCoder(GeometryCoder.of());
     * </code>
     * @return
     */
    public static ParDo.SingleOutput<Geometry, Void> wktPrinter(){
        return ParDo.of(new DoFn<Geometry, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c){
                System.out.println(WKTWriter.create(2).write(c.element()));
            }
        });
    }

    /**
     * 计数器，统计PCollection中元素个数
     * <code>
     *     PCollection<Geometry> pfs =.......
     *     PCollection<Long> c= pfs.apply(GeometryPTransforms.counter());
     * </code>
     * @return
     */
    public static PTransform<PCollection<Geometry>, PCollection<Long>> counter(){
        return  Count.globally();
    }

//
//    public static class Covers implements SerializableFunction<Geometry,Boolean> {
//
//        private static final long serialVersionUID = 1L;
//
//        @Override
//        public Boolean apply(Geometry input) {
//            return null;
//        }
//    }
//
//    public static class Partitioner <E extends Envelope, T extends Geometry>  extends PTransform<PCollection<T>,PCollectionList<T>>{
//        final PCollection<E> envelopePCollection;
//
//        public Partitioner(PCollection<E> envelopePCollection) {
//            this.envelopePCollection = envelopePCollection;
//        }
//
//        public Partitioner(@Nullable String name, PCollection<E> envelopePCollection) {
//            super(name);
//            this.envelopePCollection = envelopePCollection;
//        }
//
//        @Override
//        public PCollectionList<T> expand(PCollection<T> input) {
//            final PCollectionView<Iterable<E>> envView=envelopePCollection.apply(IterablePTransforms.<E>toIterable().asSingletonView());
//            Iterable<E> ies =null;
//
//            return input.apply(SpatialPartitioner.<E,T>createPartitionTransform(ies));
//        }
//    }
}
