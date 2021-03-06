package com.pdp.app;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import org.apache.hadoop.util.GenericOptionsParser;

import java.util.HashMap;
import java.util.Map;

import java.text.SimpleDateFormat;
import java.util.Date;


public class App {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
        private LongWritable k = new LongWritable();
        private Text v = new Text();

        public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
            String[] tokens = value.toString().split("\\s");
            if (tokens[0].charAt(0) != '#') {
                Long machine = new Long(tokens[1]);
                if (tokens[2].equals("1")) {
                    k.set(machine);
                    v.set(tokens[3] + ":" + tokens[4]);
                    output.collect(k, v);
                }
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {
        private Text val = new Text();

        public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
            Long sum = new Long(0);
            Long traceStart = new Long(Long.MAX_VALUE);
            Long traceEnd = new Long(0);
            Long start = new Long(0);
            Long end = new Long(0);
            while (values.hasNext()) {
                String line = values.next().toString();
                String[] tokens = line.split(":");
                start = new Double(tokens[0]).longValue();
                end = new Double(tokens[1]).longValue();

                if (start < traceStart) {
                    traceStart = start;
                }
                if (end > traceEnd) {
                    traceEnd = end;
                }
                sum += (end - start);
            }
            val = new Text(Long.toString(sum));
            output.collect(key, val);
        }
    }

    public static class Reduce2 extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {
        private Text val = new Text();

        public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
            Long sum = new Long(0);
            Long traceStart = new Long(Long.MAX_VALUE);
            Long traceEnd = new Long(0);
            Long start = new Long(0);
            Long end = new Long(0);
            // Associates a day to a active time
            java.util.Map<String, Long> days = new HashMap<String, Long>();
            while (values.hasNext()) {
                String line = values.next().toString();
                String[] tokens = line.split(":");
                start = new Double(tokens[0]).longValue();
                end = new Double(tokens[1]).longValue();

                if (start < traceStart) {
                    traceStart = start;
                }
                if (end > traceEnd) {
                    traceEnd = end;
                }

                // Build day label
                Date date = new Date(((long) start) * 1000L);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                String dayLabel = sdf.format(date);

                // Update day time
                Long dayTime = end - start;
                Long daySum = days.getOrDefault(dayLabel, new Long(0));
                days.put(dayLabel, daySum + dayTime);

                sum += dayTime;
            }

            // If active for 300 days
            if (days.size() >= 300) {
                // If average uptime is longer than 1 hour (3600 seconds)
                if (sum / days.size() > 3600) {
                    val = new Text("Avg uptime: " + Long.toString(sum / days.size()) + "\tStart: " + Long.toString(traceStart) + "\tEnd: " + Long.toString(traceEnd));
                    output.collect(key, val);
                }
            }
        }
    }

    public static void main( String[] args ) throws Exception
    {
        System.out.println( "Hello World!" );
        JobConf conf = new JobConf(App.class);
        conf.setJobName("tempocount");

        conf.setNumReduceTasks(Integer.parseInt(args[2]));

        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(Map.class);
        if (args[3] == "1") {
            conf.setReducerClass(Reduce.class);
        } else {
            conf.setReducerClass(Reduce2.class);
        }

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
