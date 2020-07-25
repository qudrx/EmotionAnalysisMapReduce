package org.example;

import org.ansj.recognition.impl.StopRecognition;
import org.ansj.splitWord.analysis.DicAnalysis;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.nlpcn.commons.lang.tire.domain.Forest;
import org.nlpcn.commons.lang.tire.domain.Value;
import org.nlpcn.commons.lang.tire.library.Library;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class ChineseWordAnalysis {
    /**
     * 分词，标记
     */
    private static class WordMapper extends Mapper<LongWritable, Text, Text, WordInfo> {
        private StopRecognition filter = new StopRecognition();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.getLocal(conf);
            String word_url = conf.get("word_url");
            filter.insertStopRegexes("[A-Za-z0-9]+.*", "\\-+.*");
            List<String> stop_words = getHdfsFileData(fs, word_url + File.separator + "stopwords.txt");
            List<String> nature_words = getHdfsFileData(fs, word_url + File.separator + "stopnature.txt");
//            Path path_obj = new Path(word_url + File.separator + "userLibrary.dic");
//            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fs.open(path_obj)));
            String[] strings = new String[nature_words.size()];
            nature_words.toArray(strings);
            filter.insertStopNatures(strings);
            filter.insertStopWords(stop_words);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Counter map_input_records = context
                    .getCounter("org.apache.hadoop.mapreduce.TaskCounter",
                            "MAP_INPUT_RECORDS");
            FileSystem fs = FileSystem.getLocal(context.getConfiguration());
            Path path = new Path(context.getConfiguration().get("cache_url"));
            FSDataOutputStream output = fs.create(path, true);
            String content = Long.toString(map_input_records.getValue());
            output.write(content.getBytes());
            output.flush();
            output.close();
            super.cleanup(context);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            String[] words = ToAnalysis.parse(split[1]).recognition(filter)
                    .toStringWithOutNature(" ").split(" ");
            double words_count = Double.parseDouble(words.length + ""); // **总word数量
            Map<String, Integer> stringIntegerHashMap = new HashMap<>();
            for (String word : words) {
                if (stringIntegerHashMap.containsKey(word)) {
                    Integer integer = stringIntegerHashMap.get(word);
                    integer++;
                    stringIntegerHashMap.put(word, integer);
                } else {
                    stringIntegerHashMap.put(word, 1);
                }
            }
            TreeSet<String> strings = new TreeSet<>(new TopNComparator());
            for (Map.Entry<String, Integer> entry : stringIntegerHashMap.entrySet()) {
                if(!entry.getKey().trim().isEmpty()){
                    strings.add(entry.getKey()+":"+entry.getValue());
                    if(strings.size()>10){
                        strings.pollFirst();
                    }
                }
            }
            for (String word:strings){
                String[] word_count = word.split(":");
                WordInfo wordInfo = new WordInfo();
                wordInfo.setWord(word_count[0]);
                wordInfo.setArticle_id(Long.parseLong(split[0]));
                wordInfo.setTf(1.0 * Integer.parseInt(word_count[1]) / words_count);
                context.write(new Text(word_count[0]), wordInfo);
            }
        }
    }

    /**
     * 统计词频
     */
    private static class WordReducer extends Reducer<Text, WordInfo, Text, NullWritable> {
        private long doc_sum = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.getLocal(conf);
            List<String> cache_url = getHdfsFileData(fs, conf.get("cache_url"));
            doc_sum = Long.parseLong(cache_url.get(0));
            super.setup(context);
        }

        @Override
        public void reduce(Text key, Iterable<WordInfo> values, Context context) throws IOException, InterruptedException {
            Iterator<WordInfo> iterator = values.iterator();
            Set<String> longs = new HashSet<>();
            while (iterator.hasNext()) {
                WordInfo next = iterator.next();
                longs.add(next.getArticle_id() + "," + next.getTf());
            }
            long sum_df = longs.size();
            for (String w : longs) {
                double tf = Double.parseDouble(w.split(",")[1]);
                // doc_id,word,tf-idf
                context.write(new Text(w.split(",")[0] + "," + key.toString() + "," + 1.0 * tf * Math.log(1.0 * doc_sum / sum_df)), NullWritable.get());
            }
        }
    }

    private static class GroupByKeyMapper extends Mapper<LongWritable,Text,Text,Text>{
        private Text output_key=new Text();
        private Text output_value=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String name = fileSplit.getPath().getName();
            if (name.startsWith("part-r-")) {
                String[] split = value.toString().split(",");
                output_key.set(split[0]);
                output_value.set(split[1] + ":" + split[2]);
                context.write(output_key, output_value);
            }
        }
    }

    private static class GroupByKeyReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            StringBuilder stringBuilder = new StringBuilder();
            int count=0;
            while (iterator.hasNext()) {
                String[] word = iterator.next().toString().split(":");
                stringBuilder.append(word[1]);
                stringBuilder.append(",");
                count++;
            }
            while (count<10){
               stringBuilder.append("0.0");
               stringBuilder.append(",");
               count++;
            }
            context.write(key, new Text(stringBuilder.toString()));
        }
    }

    private static List<String> getHdfsFileData(FileSystem fs, String path) {
        List<String> results = new ArrayList<>();
        Path path_obj = new Path(path);
        BufferedReader bufferedReader = null;
        try {
            InputStreamReader inputStreamReader = new InputStreamReader(fs.open(path_obj));
            bufferedReader = new BufferedReader(inputStreamReader);
            String cache_Str = "";
            while ((cache_Str = bufferedReader.readLine()) != null) {
                results.add(cache_Str);
            }
            bufferedReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (bufferedReader != null) {
                    bufferedReader.close();
                }
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
        return results;
    }

    public static void main(String[] args) throws Exception {
        args = new String[2];
        args[0] = "H:\\Work\\Large\\ll\\MapReduce情感分析\\input\\model";
        args[1] = "H:\\Work\\Large\\ll\\MapReduce情感分析\\result\\model";
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: SplitWord <in> <out> <stop_dir>");
            System.exit(2);
        }
        //本地模式运行mr,输入输出的数据可以在本地，也可以在hdfs
//        conf.set("fs.defaultFS","hdfs://node1:9000");
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        conf.set("cache_url", "H:\\Work\\Large\\ll\\MapReduce情感分析\\cache");
        conf.set("word_url", "H:\\Work\\Large\\ll\\MapReduce情感分析\\code\\EmotionAnalysisMapReduce\\src\\main\\resources\\");
        Job job1 = Job.getInstance(conf, "test"); // 设置一个用户定义的job名称
        job1.setJarByClass(ChineseWordAnalysis.class);
        job1.setMapperClass(WordMapper.class); // 为job设置Mapper类
        job1.setReducerClass(WordReducer.class); // 为job设置Reducer类
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(WordInfo.class);
        job1.setOutputKeyClass(Text.class); // 为job的输出数据设置Key类
        job1.setOutputValueClass(DoubleWritable.class); // 为job输出设置value类
        job1.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]+File.separator+"001"));// 为job设置输出路径
        boolean b = job1.waitForCompletion(true);
        if(b){
            Job job2 = Job.getInstance(conf);
            job2.setJarByClass(ChineseWordAnalysis.class);
            job2.setMapperClass(GroupByKeyMapper.class); // 为job设置Mapper类
            job2.setReducerClass(GroupByKeyReducer.class); // 为job设置Reducer类
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class); // 为job的输出数据设置Key类
            job2.setOutputValueClass(Text.class); // 为job输出设置value类
            job2.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.addInputPath(job2, new Path(otherArgs[1]+File.separator+"001"));
            FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]+File.separator+"002"));// 为job设置输出路径
            job2.waitForCompletion(true);
        }

    }
}
