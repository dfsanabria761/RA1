/*
 * Copyright 2012 Internet Archive
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package util;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.RecordReader;


/**
 *Formato de entrada que s�lo lee el nombre y la ruta del archivo. 
 */
public class FilenameInputFormat extends FileInputFormat<Text,Text>
{
@Override
protected boolean isSplitable(JobContext context, Path filename) {
	return false;
}
	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
		return new RecordReader<Text, Text>() {
			private String filename;
			private String path;
			private Text key;
			private Text value;
			@Override
			public boolean nextKeyValue() throws IOException, InterruptedException {
				if(path!=null&&key==null){
					key=new Text(filename);
					value=new Text(path);
					return true;
				}
				return false;
			}
			
			@Override
			public void initialize(InputSplit split, TaskAttemptContext attemptContext)
					throws IOException, InterruptedException {
				FileSplit f=(FileSplit) split;
				Path p=f.getPath();
				filename=p.getName();
				path=p.toString();
				key=null;
				value=null;
				
			}
			
			@Override
			public float getProgress() throws IOException, InterruptedException {
				return 0;
			}
			
			@Override
			public Text getCurrentValue() throws IOException, InterruptedException {
				return value;
			}
			
			@Override
			public Text getCurrentKey() throws IOException, InterruptedException {
				return key;
			}
			
			@Override
			public void close() throws IOException {
				
			}
			
		};
	}
  /***/
 
  
}
