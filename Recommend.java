import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Recommend {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, MapWritable>{

        private IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private HashMap<String,Integer> sub_word_count;
        String item;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            sub_word_count=new HashMap<String, Integer>();
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String line =value.toString().toLowerCase();
            line=line.replaceAll(","," ");
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                item = itr.nextToken();
                //word.set(item);
                if(sub_word_count.containsKey(item))
                {
                    int count= sub_word_count.get(item);
                    count++;
                    sub_word_count.put(item,count);
                }
                else
                {
                    sub_word_count.put(item,1);
                }
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            for (Map.Entry<String,Integer> entry:sub_word_count.entrySet())
            {
                word.set(entry.getKey());
                for(Map.Entry<String,Integer> relation_entry:sub_word_count.entrySet())
                {
                    if(entry.getKey().equals(relation_entry.getKey())==false)
                    {
                        MapWritable v=new MapWritable();
                        Text sub_key=new Text();
                        sub_key.set(relation_entry.getKey());
                        IntWritable sub_key_value = new IntWritable();
                        sub_key_value.set(relation_entry.getValue());
                        v.put(sub_key,sub_key_value);
                        context.write(word,v);
                    }
                }
            }

        }
    }

    public static class HashCombiner
            extends Reducer<Text,MapWritable,Text,MapWritable>{
        protected void reduce(Text key, Iterable<MapWritable> value,
                              Context context)
                              throws IOException, InterruptedException{
            MapWritable tags_map = new MapWritable();
            for (MapWritable val:value)
            {
                if (val.isEmpty()==false){
                    for (Writable ele:val.keySet()){
                        IntWritable cnt =(IntWritable)val.get((Text)ele);
                        if(tags_map.containsKey((ele))){
                            tags_map.put((Text)ele,new IntWritable(cnt.get()+((IntWritable)tags_map.get((Text)ele)).get()));
                        }
                        else{
                        tags_map.put(
                                (Text)ele,cnt);
                        }
                    }
                }
            }
            context.write(key,tags_map);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,MapWritable,Text,Text> {
        //private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<MapWritable> values,
                           Context context
        ) throws IOException, InterruptedException {

            Map<String,Integer> counts = new HashMap<String, Integer>();

            for (MapWritable val : values) {
               if(val.isEmpty()==false)
               {
                   for (Writable ele:val.keySet()){
                       int cnt = ((IntWritable)val.get(ele)).get();
                       String ele_Key = ((Text)ele).toString();
                       if(counts.containsKey(ele_Key)){
                           cnt+=counts.get(ele_Key);
                       }
                       counts.put(ele_Key,cnt);
                   }
               }
            }

            List<Map.Entry<String,Integer>> list = new ArrayList<Map.Entry<String, Integer>>(counts.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return o2.getValue().compareTo((o1.getValue()));
                }
            });
            String result = "";
            for(Map.Entry<String,Integer>mapping:list)
            {
                result=result+" "+mapping.getKey();
            }

            Text output = new Text(result);
            context.write(key, output);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "recommend system");
        job.setJarByClass(Recommend.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(HashCombiner.class);
        job.setReducerClass(IntSumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}