package nju;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.io.IntWritable.Comparator;
import org.apache.hadoop.io.WritableComparable;

public class CommonFriends_imporved {

  public static class CommonFriendsStep1Mapper
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String line = value.toString();
      String[] people = line.split(", ");
      String master = people[0];
      String[] friends = people[1].split(" ");
      for(String friend : friends){
        context.write(new Text(friend), new Text(master));
      }
    }
  }

  public static class CommonFriendsStep1Reducer
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text friend, Iterable<Text> masters,
                       Context context
                       ) throws IOException, InterruptedException {
      StringBuffer sBuffer = new StringBuffer();
      for(Text master: masters){
        sBuffer.append(master).append(",");
      }
      context.write(friend,new Text(sBuffer.toString()));
    }
  }

  public static class CommonFriendsStep2Mapper
       extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String line = value.toString();
      String[] people = line.split("\t");
      String friend = people[0];
      String[] masters = people[1].split(",");
      Arrays.sort(masters);
      for(int i = 0; i<masters.length-1; ++i){
        for(int j = i+1;j<masters.length;++j){
          context.write(new Text("[" + masters[i] + "，" + masters[j] + "]:"), new Text(friend));
        }
      }
    }
  }

  public static class CommonFriendsStep2Reducer
       extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text master, Iterable<Text> friends,
                       Context context
                       ) throws IOException, InterruptedException {
      StringBuffer sBuffer = new StringBuffer();
      for(Text friend: friends){
        sBuffer.append(friend).append(",");
      }
      context.write(master,new Text(sBuffer.toString()));
    }
  }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("error");
            System.exit(2);
        }
        Job job1 = Job.getInstance(conf, "common friends step1");
        job1.setJarByClass(CommonFriends_imporved.class);
        job1.setMapperClass(CommonFriendsStep1Mapper.class);
        job1.setReducerClass(CommonFriendsStep1Reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job1, new Path(otherArgs[i]));
        }
        Path intermediatePath = new Path("IntermediateOutput");
        FileOutputFormat.setOutputPath(job1,intermediatePath);
        job1.waitForCompletion(true);

        System.out.println("开始第二个任务");

        Job job2 = Job.getInstance(conf, "common friends step2");
        job2.setJarByClass(CommonFriends_imporved.class);
        job2.setMapperClass(CommonFriendsStep2Mapper.class);
        job2.setReducerClass(CommonFriendsStep2Reducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, intermediatePath);
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[otherArgs.length - 1]));
        job2.waitForCompletion(true);


        FileSystem.get(conf).delete(intermediatePath);
        System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }
}

