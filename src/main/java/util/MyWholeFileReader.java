package util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public  class MyWholeFileReader extends RecordReader<Text, Text> {

	 

    private CompressionCodecFactory compressionCodecs = null;

    private long start;

    private LineReader in;

    private Text key = null;

    private Text value = null;

    private Text buffer = new Text();



    public void initialize(InputSplit genericSplit,

                           TaskAttemptContext context) throws IOException {

        FileSplit split = (FileSplit) genericSplit;

        Configuration job = context.getConfiguration();

        start = split.getStart();

        final Path file = split.getPath();

        compressionCodecs = new CompressionCodecFactory(job);

        final CompressionCodec codec = compressionCodecs.getCodec(file);



        // open the file and seek to the start of the split

        FileSystem fs = file.getFileSystem(job);

        FSDataInputStream fileIn = fs.open(split.getPath());

        if (codec != null) {

            in = new LineReader(codec.createInputStream(fileIn), job);

          }

        else {

            in = new LineReader(fileIn, job);

        }

        if (key == null) {

            key = new Text();

        }

        key.set(split.getPath().getName());

        if (value == null) {

            value = new Text();

        }



    }



    public boolean nextKeyValue() throws IOException {

        int newSize = 0;

        StringBuilder sb = new StringBuilder();

        newSize = in.readLine(buffer);

        while (newSize > 0) {

            String str = buffer.toString();

            sb.append(str);

            sb.append("\n");

            newSize = in.readLine(buffer);

        }



        String s = sb.toString();

        value.set(s);



        if (sb.length() == 0) {

            key = null;

            value = null;

            return false;

        }

        else {

            return true;

        }

    }



    @Override

    public Text getCurrentKey() {

        return key;

    }



    @Override

    public Text getCurrentValue() {

        return value;

    }



    /**

     * Get the progress within the split

     */

    public float getProgress() {

        return 0.0f;

    }



    public synchronized void close() throws IOException {

        if (in != null) {

            in.close();

        }

    }

}



