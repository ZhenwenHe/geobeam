package cn.edu.cug.cs.gtl.beam.pcollection;

import cn.edu.cug.cs.gtl.beam.pipeline.SpatialPipeline;
import cn.edu.cug.cs.gtl.geom.Envelope;
import cn.edu.cug.cs.gtl.geom.Geometry;
import cn.edu.cug.cs.gtl.io.File;
import cn.edu.cug.cs.gtl.io.wkt.WKTReader;
import cn.edu.cug.cs.gtl.io.wkt.WKTWriter;
import org.apache.beam.sdk.io.LocalResources;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 数据预处理，主要进行数据的样本采集与写出，
 * 最大边界矩形的计算与写出。如果输入的文件
 * 为path/fileName.tsv,写出的位置关系如下：
 * path/fileName/fileName.sample  存放采样数据的文件
 *path/fileName/fileName.envelope  存放最大边界矩形的文件
 * path/fileName/fileName.count  存放所有对象个数的文件
 * path/fileName/fileName.gtype  存放几何类型的文件
 * path/fileName/fileName.tsv  处理后的数据文件
 */
public class Preprocesser implements Serializable{

    private static final long serialVersionUID = 2600194278485489215L;

    private static final Logger LOG = LoggerFactory.getLogger(Preprocesser.class);


    protected final String inputDataFile;
    protected final String outputDataFile;
    protected transient final SpatialPipeline pipeline;
    protected int sampleNumber=-1;
    protected int geometryType=-1;
    protected int partitionType= -1;
    protected Envelope totalExtent=null;
    protected List<Geometry> samples=null;
    protected long totalCount=-1;




    public static Preprocesser create(String inputDataFile, int geomType, int partitionType , int samples,String outputDataFile, boolean overwrite, SpatialPipeline pipeline) {
        return new Preprocesser(inputDataFile,geomType,  partitionType , samples,outputDataFile,overwrite,pipeline);
    }

    /**
     * 从已经处理过的数据中构建Preprocesser,其前提条件是必须先运行过Preprocesser进行过预处理
     * @param inputDataFile
     * @param pipeline
     * @return
     */
    public static Preprocesser load(String inputDataFile,SpatialPipeline pipeline) {
        Preprocesser p= new Preprocesser(inputDataFile,pipeline);
        try {
            if(p.checkFiles()==false)
                throw new IllegalStateException("please run Preprocesser to generater preprocesser files");
            p.getGeometryType();
            p.getSampleNumber();
            p.getTotalExtent();
        }
        catch (IllegalStateException e){
            e.printStackTrace();
        }
        return p;
    }

    protected Preprocesser(String inputDataFile,SpatialPipeline pipeline){
        String fileName = File.getFileNameWithoutSuffix(inputDataFile);
        String path = File.getDirectory(inputDataFile);
        path = path+File.separator+fileName;
        this.outputDataFile=path+File.separator+File.getFileName(inputDataFile);
        this.inputDataFile = inputDataFile;
        this.pipeline = pipeline;
    }
    protected Preprocesser(String inputDataFile, int geomType, int partitionType,int samples,String outputDataFile, boolean overwrite, SpatialPipeline pipeline) {
        this.inputDataFile = inputDataFile;
        this.pipeline = pipeline;
        this.sampleNumber=samples;
        this.geometryType=geomType;
        this.partitionType=partitionType;
        if(outputDataFile.isEmpty()){
            String fileName = File.getFileNameWithoutSuffix(inputDataFile);
            String path = File.getDirectory(inputDataFile);
            path = path+File.separator+fileName;
            this.outputDataFile=path+File.separator+File.getFileName(inputDataFile);
        }
        else
            this.outputDataFile=outputDataFile;

        if(checkFiles()==false || overwrite==true)
            run();

    }

    public String getInputDataFile() {
        return inputDataFile;
    }

    public String getOutputDataFile() {
        return outputDataFile;
    }

    public String getGTypeFile(){
        return File.replaceSuffixName(outputDataFile,"gtype");
    }

    public String getPTypeFile(){
        return File.replaceSuffixName(outputDataFile,"ptype");
    }

    public String getEnvelopeFile(){
        return File.replaceSuffixName(outputDataFile,"envelope");
    }

    public String getSampleFile(){
        return File.replaceSuffixName(outputDataFile,"sample");
    }

    public String getCountFile(){
        return File.replaceSuffixName(outputDataFile,"count");
    }

    public int getSampleNumber() {
        if(this.sampleNumber<0){
            //read from file
            List<Geometry> ls = getSampleList();
            if(ls!=null) {
                sampleNumber = ls.size();
            }
        }
        return sampleNumber;
    }

    /**
     * 获取样本列表，这里是从本地文件读取的，可能存在问题；
     * 需要添加从HDFS上读取功能
     * @return
     */
    public List<Geometry> getSampleList() {
        if(this.samples==null){
            String sampleFile=getSampleFile();
            File file = new File(sampleFile);
            if(file.exists()){
                List<String> c= File.readTextLines(file);
                WKTReader wktReader = WKTReader.create();
                samples = new ArrayList<>(c.size());
                for(String s : c){
                    samples.add(wktReader.read(s));
                }
                return samples;
            }
        }
        return samples;
    }

    /**
     * * 获取几何类型，这里是从本地文件读取的，可能存在问题；
     * 需要添加从HDFS上读取功能
     * @return
     */
    public int getGeometryType() {
        if(this.geometryType<0){
            //read from file
            String geomFile = getGTypeFile();
            File file = new File(geomFile);
            if(file.exists()){
                List<String> c= File.readTextLines(file);
                geometryType= Integer.valueOf(c.get(0)).intValue();
            }
        }
        return geometryType;
    }

    /**
     * * 获取划分类型，这里是从本地文件读取的，可能存在问题；
     * 需要添加从HDFS上读取功能
     * @return
     */
    public int getPartitionType() {
        if(this.partitionType<0){
            //read from file
            String geomFile = getPTypeFile();
            File file = new File(geomFile);
            if(file.exists()){
                List<String> c= File.readTextLines(file);
                this.partitionType= Integer.valueOf(c.get(0)).intValue();
            }
        }
        return partitionType;
    }

    /**
     * 获取对象总数，这里是从本地文件读取的，可能存在问题；
     * 需要添加从HDFS上读取功能
     * @return
     */
    public long getTotalCount(){
        if(this.totalCount<0) {
            String countFile = File.replaceSuffixName(outputDataFile, "count");
            File file = new File(countFile);
            if (file.exists()) {
                List<String> c = File.readTextLines(file);
                totalCount= Long.valueOf(c.get(0)).longValue();
            }
        }
        return this.totalCount;
    }

    /**
     * 获取全体对象的最大包围矩形，这里是从本地文件读取的，可能存在问题；
     * 需要添加从HDFS上读取功能
     * @return
     */
    public Envelope getTotalExtent(){
        if(this.totalExtent==null){
            String envelopeFile = getEnvelopeFile();
            File file = new File(envelopeFile);
            if(file.exists()){
                List<String> c= File.readTextLines(file);
                totalExtent= Envelope.fromString(c.get(0));
            }
        }
        return totalExtent;
    }

    /**
     * 加载数据进行预处理，生成预处理结果文件
     */
    private void run( ){
        if(checkFiles())
            return;

        String fileType = File.getSuffixName(inputDataFile);
        final SpatialPipeline p = this.pipeline;
        switch (this.geometryType){
            case Geometry.POINT:{
                PointPCollection geoms = new PointPCollection(p);
                if(fileType.equals("csv")){
                    geoms.loadFromCSVFile(inputDataFile);
                }
                else if(fileType.equals("tsv")){
                    geoms.loadFromTSVFile(inputDataFile);
                }
                else if(fileType.equals("shp")){
                    geoms.loadFromSHPFile(inputDataFile);
                }
                else{
                    try {
                        throw new IllegalArgumentException("wrong file type in preprocesser");
                    }
                    catch (IllegalStateException e){
                        e.printStackTrace();
                    }
                }
                preprocessing(geoms);
                break;
            }
            case Geometry.LINESTRING:{
                LineStringPCollection geoms = new LineStringPCollection(p);
                if(fileType.equals("csv")){
                    geoms.loadFromCSVFile(inputDataFile);
                }
                else if(fileType.equals("tsv")){
                    geoms.loadFromTSVFile(inputDataFile);
                }
                else if(fileType.equals("shp")){
                    geoms.loadFromSHPFile(inputDataFile);
                }
                else{
                    try {
                        throw new IllegalArgumentException("wrong file type in preprocesser");
                    }
                    catch (IllegalStateException e){
                        e.printStackTrace();
                    }
                }
                preprocessing(geoms);
                break;
            }
            case Geometry.POLYGON:{
                PolygonPCollection geoms = new PolygonPCollection(p);
                if(fileType.equals("csv")){
                    geoms.loadFromCSVFile(inputDataFile);
                }
                else if(fileType.equals("tsv")){
                    geoms.loadFromTSVFile(inputDataFile);
                }
                else if(fileType.equals("shp")){
                    geoms.loadFromSHPFile(inputDataFile);
                }
                else{
                    try {
                        throw new IllegalArgumentException("wrong file type in preprocesser");
                    }
                    catch (IllegalStateException e){
                        e.printStackTrace();
                    }
                }
                preprocessing(geoms);
                break;
            }
            case Geometry.MULTIPOINT:{
                MultiPointPCollection geoms = new MultiPointPCollection(p);
                if(fileType.equals("csv")){
                    geoms.loadFromCSVFile(inputDataFile);
                }
                else if(fileType.equals("tsv")){
                    geoms.loadFromTSVFile(inputDataFile);
                }
                else if(fileType.equals("shp")){
                    geoms.loadFromSHPFile(inputDataFile);
                }
                else{
                    try {
                        throw new IllegalArgumentException("wrong file type in preprocesser");
                    }
                    catch (IllegalStateException e){
                        e.printStackTrace();
                    }
                }
                preprocessing(geoms);
                break;
            }
            case Geometry.MULTILINESTRING:{
                MultiLineStringPCollection geoms = new MultiLineStringPCollection(p);
                if(fileType.equals("csv")){
                    geoms.loadFromCSVFile(inputDataFile);
                }
                else if(fileType.equals("tsv")){
                    geoms.loadFromTSVFile(inputDataFile);
                }
                else if(fileType.equals("shp")){
                    geoms.loadFromSHPFile(inputDataFile);
                }
                else{
                    try {
                        throw new IllegalArgumentException("wrong file type in preprocesser");
                    }
                    catch (IllegalStateException e){
                        e.printStackTrace();
                    }
                }
                preprocessing(geoms);
                break;
            }
            case Geometry.MULTIPOLYGON:{
                MultiPolygonPCollection geoms = new MultiPolygonPCollection(p);
                if(fileType.equals("csv")){
                    geoms.loadFromCSVFile(inputDataFile);
                }
                else if(fileType.equals("tsv")){
                    geoms.loadFromTSVFile(inputDataFile);
                }
                else if(fileType.equals("shp")){
                    geoms.loadFromSHPFile(inputDataFile);
                }
                else{
                    try {
                        throw new IllegalArgumentException("wrong file type in preprocesser");
                    }
                    catch (IllegalStateException e){
                        e.printStackTrace();
                    }
                }
                preprocessing(geoms);
                break;
            }
            case Geometry.LINEARRING:{
                LinearRingPCollection geoms = new LinearRingPCollection(p);
                if(fileType.equals("csv")){
                    geoms.loadFromCSVFile(inputDataFile);
                }
                else if(fileType.equals("tsv")){
                    geoms.loadFromTSVFile(inputDataFile);
                }
                else if(fileType.equals("shp")){
                    geoms.loadFromSHPFile(inputDataFile);
                }
                else{
                    try {
                        throw new IllegalArgumentException("wrong file type in preprocesser");
                    }
                    catch (IllegalStateException e){
                        e.printStackTrace();
                    }
                }
                preprocessing(geoms);
                break;
            }
            case Geometry.GEOMETRYCOLLECTION:{
                try {
                    throw new IllegalArgumentException("wrong geometry type in preprocesser");
                }
                catch (IllegalStateException e){
                    e.printStackTrace();
                }
                break;
            }
        }
    }

    /**
     * 根据传入的PCollection，生成预处理结果文件
     * @param gpc
     */
    private void preprocessing(GeometryPCollection gpc){
        final WKTWriter wktWriter =  WKTWriter.create(2);
        //write geometry file
        gpc.storeToTSVFile(outputDataFile);
        //write geometry count
        {
            String countFile = getCountFile();
            ResourceId rid = LocalResources.fromString(countFile,false);
            PCollection<Long> cs = gpc.count();
            cs.apply(MapElements.via(new SimpleFunction<Long,String>() {
                @Override
                public String apply(Long g){
                    return g.toString();
                }
            })).apply(TextIO.write().to(rid).withoutSharding());
        }
        //write geometry envelope
        {
            String envelopeFile = getEnvelopeFile();
            ResourceId rid = LocalResources.fromString(envelopeFile,false);
            gpc.getEnvelopes().combine().apply(MapElements.via(new SimpleFunction<Envelope,String>() {
                @Override
                public String apply(Envelope g){
                    return g.toString();
                }
            })).apply(TextIO.write().to(rid).withoutSharding());
        }
        //write geometry sample
        {
            String sampleFile = getSampleFile();
            ResourceId rid = LocalResources.fromString(sampleFile,false);
            gpc.sampling(this.sampleNumber,rid);
        }
        //write geometry type
        {
            String geomTypeFile = getGTypeFile();
            ArrayList<Long> al = new ArrayList<>(1);
            al.add(Long.valueOf(this.geometryType));
            this.pipeline.apply(Create.<Long>of(al))
                    .apply(MapElements.via(new SimpleFunction<Long,String>() {
                        @Override
                        public String apply(Long g){
                            return g.toString();
                        }
                    })).apply(TextIO.write().to(geomTypeFile).withoutSharding());
        }
        //write partition type
        {
            String partitionTypeFile = getPTypeFile();
            ArrayList<Integer> al = new ArrayList<>(1);
            al.add(Integer.valueOf(this.partitionType));
            this.pipeline.apply(Create.<Integer>of(al))
                    .apply(MapElements.via(new SimpleFunction<Integer,String>() {
                        @Override
                        public String apply(Integer g){
                            return g.toString();
                        }
                    })).apply(TextIO.write().to(partitionTypeFile).withoutSharding());
        }

    }

    /**
     * 检查预处理结果文件是否存在
     * @return
     */
    public boolean checkFiles(){

        String countFile = getCountFile();
        String envelopeFile = getEnvelopeFile();
        String sampleFile = getSampleFile();
        String gtypeFile = getGTypeFile();
        String ptypeFile = getPTypeFile();

        return new File(outputDataFile).exists()
                && new File(countFile).exists()
                && new File(envelopeFile).exists()
                && new File(sampleFile).exists()
                && new File(gtypeFile).exists()
                && new File(ptypeFile).exists();

    }
}
