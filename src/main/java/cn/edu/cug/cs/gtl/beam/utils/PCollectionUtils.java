package cn.edu.cug.cs.gtl.beam.utils;

import cn.edu.cug.cs.gtl.beam.ptransform.EnvelopePTransforms;
import cn.edu.cug.cs.gtl.beam.ptransform.FeaturePTransforms;
import cn.edu.cug.cs.gtl.beam.ptransform.FeatureTypePTransforms;
import cn.edu.cug.cs.gtl.beam.ptransform.StringPTransforms;
import cn.edu.cug.cs.gtl.config.Config;
import cn.edu.cug.cs.gtl.feature.Feature;
import cn.edu.cug.cs.gtl.feature.FeatureType;
import cn.edu.cug.cs.gtl.geom.Envelope;
import cn.edu.cug.cs.gtl.io.File;
import cn.edu.cug.cs.gtl.io.FileDataSplitter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class PCollectionUtils {


    public static <T> List<T> collect(PCollection<T> pc){
        pc.getClass().getGenericInterfaces();
        return null;
    }

    /**
     *
     * @param envs
     * @return
     */
    public static List<Envelope> collectEnvelopes(PCollection<Envelope> envs){
        //write to local swap file
        final  String swapFileName = Config.getLocalSwapFile();
        envs.apply(EnvelopePTransforms.toByteString())
                .setCoder(StringUtf8Coder.of())
                .apply(StringPTransforms.writer(swapFileName));
        envs.getPipeline().run().waitUntilFinish();
        try {
            // wait until the swapFile exists
            File swapFile = new File(swapFileName);
            while (!swapFile.exists()) Thread.sleep(10);
            //read the local swap file and builder envelope
            BufferedReader bf= new BufferedReader(new FileReader(swapFileName));
            List<String> ss = bf.lines().collect(Collectors.toList());
            bf.close();
            ArrayList<Envelope> es = new ArrayList<>(ss.size());
            for(String s:ss){
                if(!s.isEmpty()){
                    Envelope e = new Envelope();
                    e.loadFromByteString(s);
                    es.add(e);
                }
            }
            return es;
        }
        catch (IOException|InterruptedException e){
            e.printStackTrace();
        }
        return null;
    }

    public static List<FeatureType> collectFeatureTypes(PCollection<FeatureType> fts){
        //write to local swap file
        final  String swapFileName = Config.getLocalSwapFile();
        fts.apply(FeatureTypePTransforms.toByteString())
                .setCoder(StringUtf8Coder.of())
                .apply(StringPTransforms.writer(swapFileName));
        fts.getPipeline().run().waitUntilFinish();
        try {
            // wait until the swapFile exists
            File swapFile = new File(swapFileName);
            while (!swapFile.exists()) Thread.sleep(10);
            //read the local swap file and builder envelope
            BufferedReader bf= new BufferedReader(new FileReader(swapFileName));
            List<String> ss = bf.lines().collect(Collectors.toList());
            bf.close();
            ArrayList<FeatureType> es = new ArrayList<>(ss.size());
            for(String s:ss){
                if(!s.isEmpty()) {
                    FeatureType ft = new FeatureType();
                    ft.loadFromByteString(s);
                    es.add(ft);
                }
            }
            return es;
        }
        catch (IOException | InterruptedException e){
            e.printStackTrace();
        }
        return null;
    }

    public static List<Feature> collectFeatures(PCollection<Feature> fs){
        //write to local swap file
        final  String swapFileName = Config.getLocalSwapFile();
        fs.apply(FeaturePTransforms.toByteString())
                .setCoder(StringUtf8Coder.of())
                .apply(StringPTransforms.writer(swapFileName));
        fs.getPipeline().run().waitUntilFinish();
        try {
            // wait until the swapFile exists
            File swapFile = new File(swapFileName);
            while (!swapFile.exists()) Thread.sleep(10);
            //read the local swap file and builder envelope
            BufferedReader bf= new BufferedReader(new FileReader(swapFileName));
            List<String> ss = bf.lines().collect(Collectors.toList());
            bf.close();
            ArrayList<Feature> es = new ArrayList<>(ss.size());
            for(String s:ss){
                if(!s.isEmpty()) {
                    Feature ft = new Feature();
                    ft.loadFromByteString(s);
                    es.add(ft);
                }
            }
            return es;
        }
        catch (IOException | InterruptedException e){
            e.printStackTrace();
        }
        return null;
    }

    public static List<String> collectStrings(PCollection<String> fts){
        //write to local swap file
        final  String swapFileName = Config.getLocalSwapFile();
        fts.apply(StringPTransforms.writer(swapFileName));
        fts.getPipeline().run().waitUntilFinish();
        try {
            // wait until the swapFile exists
            File swapFile = new File(swapFileName);
            while (!swapFile.exists()) Thread.sleep(10);
            //read the local swap file and builder feature type
            BufferedReader bf= new BufferedReader(new FileReader(swapFileName));
            return bf.lines().collect(Collectors.toList());
        }
        catch (IOException|InterruptedException e){
            e.printStackTrace();
        }
        return null;
    }
}
