package org.example;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class HdfsUtils {
    public static List<String> getHdfsFileData(FileSystem fs, Path path_obj) {
        List<String> results = new ArrayList<>();
        BufferedReader bufferedReader = null;
        try {
            InputStreamReader inputStreamReader = new InputStreamReader(fs.open(path_obj));
            bufferedReader = new BufferedReader(inputStreamReader);
            String cache_Str = "";
            while ((cache_Str = bufferedReader.readLine()) != null) {
                results.add(cache_Str);
            }
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

}
