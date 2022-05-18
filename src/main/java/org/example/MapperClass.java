package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import static java.lang.String.format;

public class MapperClass extends Mapper<LongWritable, Text, Text, Text>{

    private static final String PATTERNDELIMITER = ",:;";
    public static final int CHANNELIDIDX = 3;
    public static final int VIEWIDX = 7;
    public static final int LIKEIDX = 8;
    public static final int DISLIKESIDX = 9;
    public static final int COMMENTCOUNTIDX = 10;
    private Text channelIdViewsLikesDislikesCommentCount = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        List<String> csvRecordsOfLine = getRecordsFromLine(value.toString());
        try
        {
            String channelId = csvRecordsOfLine.get(CHANNELIDIDX);
            String views = csvRecordsOfLine.get(VIEWIDX);
            String likes = csvRecordsOfLine.get(LIKEIDX);
            String dislikes = csvRecordsOfLine.get(DISLIKESIDX);
            String commentCount = csvRecordsOfLine.get(COMMENTCOUNTIDX);
            channelIdViewsLikesDislikesCommentCount.set(format("%s-%s-%s-%s", views, likes, dislikes, commentCount));
            context.write(new Text(channelId), channelIdViewsLikesDislikesCommentCount);
        }
        catch (IndexOutOfBoundsException ex)
        {
           //
        }
    }

    private List<String> getRecordsFromLine(String line)
    {
        List<String> values = new ArrayList<>();
        try (Scanner rowScanner = new Scanner(line))
        {
            rowScanner.useDelimiter(PATTERNDELIMITER);
            while (rowScanner.hasNext())
            {
                values.add(rowScanner.next());
            }
        }
        return values;
    }
}
