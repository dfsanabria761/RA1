package util;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**

* Splitter that reads a whole file as a single record

* This is useful when you have a large number of files

* each of which is a complete unit - for example XML Documents

*/

public class WholeFileInputFormat extends FileInputFormat<Text, Text> {

 

    @Override

    public RecordReader<Text, Text>  createRecordReader(InputSplit split,

                       TaskAttemptContext context) {

        return new MyWholeFileReader();

    }

 

    @Override

    protected boolean isSplitable(JobContext context, Path file) {

        return false;

    }

}