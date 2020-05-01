package cn.edu.cug.cs.gtl.beam.io.tsv;

import com.google.common.base.Joiner;
import cn.edu.cug.cs.gtl.io.FileDataSplitter;
import org.apache.beam.sdk.io.FileIO;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.List;

public class TSVSink implements FileIO.Sink<List<String>> {

    private String header;
    private static final  String delimiter=FileDataSplitter.TSV.getDelimiter();
    private PrintWriter writer;

    public TSVSink(List<String> colNames) {
      this.header = Joiner.on(delimiter).join(colNames);
    }

    public TSVSink() {
        this.header = null;
    }

    @Override
    public void open(WritableByteChannel channel) throws IOException {
        writer = new PrintWriter(Channels.newOutputStream(channel));
        if(header!=null){
            if(header.length()>0) writer.println(header);
        }
    }

    @Override
    public void write(List<String> element) throws IOException {
        writer.println(Joiner.on(delimiter).join(element));
    }

    @Override
    public void flush() throws IOException {
        writer.flush();
    }
}
