package org.example;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import static java.lang.String.format;

public class ReducerClass extends Reducer<Text, Text, Text, NullWritable>{
    private static final String PATTERNDELIMITER = "-";
    public static final int VIEWIDX = 0;
    public static final int LIKEIDX = 1;
    public static final int DISLIKEIDX = 2;
    public static final int COMMENTCOUNTIDX = 3;

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long viewsCount = 0L;
        long likeCount = 0L;
        long dislikeCount = 0L;
        long commentCount = 0L;
        for (Text x : values) {
            String value = x.toString();
            String[] splitScores = value.split(PATTERNDELIMITER);
            try {
                long viewEntry = Long.parseLong(splitScores[VIEWIDX]);
                long likesEntry = Long.parseLong(splitScores[LIKEIDX]);
                long dislikesEntry = Long.parseLong(splitScores[DISLIKEIDX]);
                long commentCountEntry = Long.parseLong(splitScores[COMMENTCOUNTIDX]);
                viewsCount += viewEntry;
                likeCount += likesEntry;
                dislikeCount += dislikesEntry;
                commentCount += commentCountEntry;
            } catch (NumberFormatException ex) {
                //
            }
        }
        context.write(new Text(format("%s,%s,%s,%s,%s", key, viewsCount, likeCount, dislikeCount, commentCount)), null);
    }
}
