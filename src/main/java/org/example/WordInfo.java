package org.example;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class WordInfo implements WritableComparable {
    private String word;
    private long article_id;
    private double tf;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public long getArticle_id() {
        return article_id;
    }

    public void setArticle_id(long article_id) {
        this.article_id = article_id;
    }

    public double getTf() {
        return tf;
    }

    public void setTf(double tf) {
        this.tf = tf;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.word);
        out.writeLong(this.article_id);
        out.writeDouble(this.tf);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.word=in.readUTF();
        this.article_id=in.readLong();
        this.tf=in.readDouble();
    }

    @Override
    public String toString() {
        return word + "," + article_id + "," + tf;
    }

    @Override
    public boolean equals(Object o) {
        if(o == null) return false;
        WordInfo wordInfo = (WordInfo) o;
        if(word==null && wordInfo.word==null && article_id == wordInfo.article_id){
            return true;
        }
        if(word==null&& wordInfo.word!=null){
            return false;
        }
        return article_id == wordInfo.article_id &&
                Double.compare(wordInfo.tf, tf) == 0 &&
                word.equals(wordInfo.word);
    }

    @Override
    public int hashCode() {
        return Objects.hash(word, article_id, tf);
    }

    @Override
    public int compareTo(Object o) {
        WordInfo wordInfo = (WordInfo) o;
        return this.compareTo(wordInfo);
    }
}
