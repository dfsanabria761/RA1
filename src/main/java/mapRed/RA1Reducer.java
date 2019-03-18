package mapRed;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class RA1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	private static final Logger LOG = Logger.getLogger(RA1Mapper.class.getName());

	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context)
			throws IOException, InterruptedException {
		int tot=0;
		for(IntWritable iw:values){
			tot+=iw.get();
		}

		context.write(key, new IntWritable(tot));
		
	}

}