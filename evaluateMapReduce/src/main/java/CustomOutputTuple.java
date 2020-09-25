import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomOutputTuple implements Writable{
    private Double averageDiff;
    private Double RMS;
    private int TrainComedyCount;
    private int TrainRomanceCount;
    private int TrainActionCount;
    private int TrainDramaCount;
    private int TrainTotal;
    private int TestComedyCount;
    private int TestRomanceCount;
    private int TestActionCount;
    private int TestDramaCount;
    private int TestTotal;
    private double rating;
    private String title;
    private String genres;
    private String type;


    public CustomOutputTuple(){ }

    public CustomOutputTuple(Double averageDiff, Double RMS, int trainComedyCount, int trainRomanceCount, int trainActionCount, int trainDramaCount, int trainTotal, int testComedyCount, int testRomanceCount, int testActionCount, int testDramaCount, int testTotal, double rating, String title, String genres, String type) {
        this.averageDiff = averageDiff;
        this.RMS = RMS;
        TrainComedyCount = trainComedyCount;
        TrainRomanceCount = trainRomanceCount;
        TrainActionCount = trainActionCount;
        TrainDramaCount = trainDramaCount;
        TrainTotal = trainTotal;
        TestComedyCount = testComedyCount;
        TestRomanceCount = testRomanceCount;
        TestActionCount = testActionCount;
        TestDramaCount = testDramaCount;
        TestTotal = testTotal;
        this.rating = rating;
        this.title = title;
        this.genres = genres;
        this.type = type;
    }

    @Override
    public String toString() {
        return "averageDiff=" + averageDiff +
                ", RMS=" + RMS;
    }

    public void write(DataOutput out) throws IOException {
        out.writeDouble(averageDiff);
        out.writeDouble(RMS);
        out.writeInt(TrainComedyCount);
        out.writeInt(TrainRomanceCount);
        out.writeInt(TrainActionCount);
        out.writeInt(TrainDramaCount);
        out.writeInt(TrainTotal);
        out.writeInt(TestComedyCount);
        out.writeInt(TestRomanceCount);
        out.writeInt(TestActionCount);
        out.writeInt(TestDramaCount);
        out.writeInt(TestTotal);
        out.writeDouble(rating);
        out.writeUTF(title);
        out.writeUTF(genres);
        out.writeUTF(type);
    }

    public void readFields(DataInput in) throws IOException {
        averageDiff = in.readDouble();
        RMS = in.readDouble();
        TrainComedyCount = in.readInt();
        TrainRomanceCount = in.readInt();
        TrainActionCount = in.readInt();
        TrainDramaCount = in.readInt();
        TrainTotal = in.readInt();
        TestComedyCount = in.readInt();
        TestRomanceCount = in.readInt();
        TestActionCount = in.readInt();
        TestDramaCount = in.readInt();
        TestTotal = in.readInt();
        rating = in.readDouble();
        title = in.readUTF();
        genres = in.readUTF();
        type = in.readUTF();
    }

    public String getType() {
        return type;
    }
    public void setType(String type) {
        this.type = type;
    }
    public Double getAverageDiff() {
        return averageDiff;
    }
    public void setAverageDiff(Double averageDiff) {
        this.averageDiff = averageDiff;
    }
    public Double getRMS() {
        return RMS;
    }
    public void setRMS(Double RMS) {
        this.RMS = RMS;
    }
    public int getTrainComedyCount() {
        return TrainComedyCount;
    }
    public void setTrainComedyCount(int trainComedyCount) {
        TrainComedyCount = trainComedyCount;
    }
    public int getTrainRomanceCount() {
        return TrainRomanceCount;
    }
    public void setTrainRomanceCount(int trainRomanceCount) {
        TrainRomanceCount = trainRomanceCount;
    }
    public int getTrainActionCount() {
        return TrainActionCount;
    }
    public void setTrainActionCount(int trainActionCount) {
        TrainActionCount = trainActionCount;
    }
    public int getTrainDramaCount() {
        return TrainDramaCount;
    }
    public void setTrainDramaCount(int trainDramaCount) {
        TrainDramaCount = trainDramaCount;
    }
    public int getTrainTotal() {
        return TrainTotal;
    }
    public void setTrainTotal(int trainTotal) {
        TrainTotal = trainTotal;
    }
    public int getTestComedyCount() {
        return TestComedyCount;
    }
    public void setTestComedyCount(int testComedyCount) {
        TestComedyCount = testComedyCount;
    }
    public int getTestRomanceCount() {
        return TestRomanceCount;
    }
    public void setTestRomanceCount(int testRomanceCount) {
        TestRomanceCount = testRomanceCount;
    }
    public int getTestActionCount() {
        return TestActionCount;
    }
    public void setTestActionCount(int testActionCount) {
        TestActionCount = testActionCount;
    }
    public int getTestDramaCount() {
        return TestDramaCount;
    }
    public void setTestDramaCount(int testDramaCount) {
        TestDramaCount = testDramaCount;
    }
    public int getTestTotal() {
        return TestTotal;
    }
    public void setTestTotal(int testTotal) {
        TestTotal = testTotal;
    }
    public double getRating() {
        return rating;
    }
    public void setRating(double rating) {
        this.rating = rating;
    }
    public String getTitle() {
        return title;
    }
    public void setTitle(String title) {
        this.title = title;
    }
    public String getGenres() {
        return genres;
    }
    public void setGenres(String genres) {
        this.genres = genres;
    }
}
