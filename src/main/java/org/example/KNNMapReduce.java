package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;

public class KNNMapReduce extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job knn = Job.getInstance(getConf(), "Knn");
        knn.setJarByClass(KNNMapReduce.class);
        knn.setMapperClass(KnnMapper.class);
        knn.setReducerClass(KNNReducer.class);
        knn.setCombinerClass(KNNCombiner.class);
        knn.setMapOutputKeyClass(Text.class);
        knn.setMapOutputValueClass(Text.class);
        knn.setOutputKeyClass(Text.class);
        knn.setOutputKeyClass(Text.class);
        knn.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(knn,new Path(args[0]));
        FileOutputFormat.setOutputPath(knn,new Path(args[1]));
        return knn.waitForCompletion(true)?0:1;
    }

    private static class KnnMapper extends Mapper<LongWritable, Text,Text,Text>{
        private List<String> test_data;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
            FileSystem local = FileSystem.getLocal(configuration);
            test_data = HdfsUtils.getHdfsFileData(local, new Path(configuration.get("test.data")));
        }
        private Text mykey=new Text();
        private Text myval=new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] array = value.toString().split("\t");
            String[] w = array[1].split(",");
            for (int i = 0; i < test_data.size(); i++) {
                String[] c= test_data.get(i).split("\t")[1].split(",");
                double plog=0.0;
                for (int j = 0; j < c.length-1; j++) {
                    plog += Math.pow((Double.parseDouble(c[j])-Double.parseDouble(w[j])), 2);
                }
                plog=Math.sqrt(plog);
                mykey.set(test_data.get(i).split("\t")[0]);
                myval.set(array[2]+":"+plog);
                context.write(mykey, myval);
            }
        }
    }
    private static class KNNCombiner extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            TreeSet<String> strings = new TreeSet<>(new TopNDoubleComparator());
            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()){
               strings.add(iterator.next().toString());
               if(strings.size()>3){
                   strings.pollFirst();
               }
            }
            for (String value:strings){
                context.write(key,new Text(value));
            }
        }
    }
    private static class KNNReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            TreeSet<String> strings = new TreeSet<>(new TopNDoubleComparator());
            Map<String, Integer> stringIntegerHashMap = new HashMap<>();
            while (iterator.hasNext()){
                strings.add(iterator.next().toString());
                if(strings.size()>3){
                    strings.pollFirst();
                }
            }
            strings.forEach(s->{
                String id = s.split(":")[0];
                if(!stringIntegerHashMap.containsKey(id)){
                    stringIntegerHashMap.put(id,1);
                }else {
                    Integer integer = stringIntegerHashMap.get(id);
                    integer++;
                    stringIntegerHashMap.put(id,integer);
                }
            });
            Map.Entry<String, Integer> max =
                    stringIntegerHashMap.entrySet().stream().max(Comparator.comparingInt(Map.Entry::getValue)).get();
            context.write(key,new Text(max.getKey()));
        }
    }

    public static void main(String[] args) {
        try {
            args=new String[2];
            args[0]="H:\\Work\\Large\\ll\\MapReduce情感分析\\result\\knn\\input";
            args[1]="H:\\Work\\Large\\ll\\MapReduce情感分析\\result\\knn\\output";
            Configuration configuration = new Configuration();
            configuration.set("test.data","H:\\Work\\Large\\ll\\MapReduce情感分析\\result\\test\\002\\part-r-00000");
            configuration.set("fs.defaultFS", "file:///");
            configuration.set("mapreduce.framework.name", "local");
            ToolRunner.run(configuration,new KNNMapReduce(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
