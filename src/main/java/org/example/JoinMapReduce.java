package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

public class JoinMapReduce {
    private static class JoinMapper extends Mapper<LongWritable, Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit= (FileSplit) context.getInputSplit();
            String name = fileSplit.getPath().getName();
            String[] split = value.toString().split("\t");
            String output_key=split[0];
            String output_value="";
            if(name.startsWith("model_tfidf")){
                output_value="model_tfidf"+":"+split[1];
            }else {
                if(split[2].trim().isEmpty()){
                    output_value="model_category"+":"+-1;
                }else {
                    output_value="model_category"+":"+split[2];
                }
            }
            context.write(new Text(output_key),new Text(output_value));
        }
    }
    private static class JoinReducer extends Reducer<Text,Text,Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            String tf_idf="";
            String category="";
            while (iterator.hasNext()){
                String next = iterator.next().toString();
                if(next.startsWith("model_tfidf")){
                    tf_idf=next.split(":")[1];
                }else {
                    category=next.split(":")[1];
                }
            }
            if(!category.equals("-1")){
                context.write(key,new Text(tf_idf+"\t"+category));
            }
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "file:///");
        configuration.set("mapreduce.framework.name", "local");
        Job job = Job.getInstance(configuration, "JoinMapReduce");

        job.setJarByClass(JoinMapReduce.class);
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job,new Path("H:\\Work\\Large\\ll\\MapReduce情感分析\\model"));
        FileOutputFormat.setOutputPath(job,new Path("H:\\Work\\Large\\ll\\MapReduce情感分析\\result\\knn\\input"));
        job.waitForCompletion(true);

    }
}
