package mr;

import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.Serializable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.Collections;

import mr.io.TextWritable;

public abstract class Reducer<K1, V1, K2, V2> implements Serializable {
    private static final long serialVersionUID = 99l;

    private String dirname;
    private HashMap<String, Integer> skipCounts;
    private LinkedList<Record> records;

    public Reducer(String dirname)
    {
        this.dirname = dirname;
        skipCounts = new HashMap<String, Integer>();
        records = new LinkedList<Record>();
    }

    public void bootstrap() {
        /* insert one record/file into recordQueue */
        File dir = new File(dirname);
        File[] files = dir.listFiles();
        for (File file : files) {
            String filename = file.getName();
            skipCounts.put(filename, 1);
            try {
                BufferedReader br = new BufferedReader(new FileReader(file));
                String line = br.readLine();
                if (line != null) {
                    /* skip empty file */
                    String[] words = line.split(" ");
                    /* HARD CODE TYPE TO BE TEXTWRITABLE */
                    TextWritable key = new TextWritable();
                    TextWritable value = new TextWritable();
                    key.setVal(words[0]);
                    value.setVal(words[1]);
                    records.add(new Record(key, value, filename));
                }
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
            }
        }
        Collections.sort(records);
    }

    public Record getNext() throws IOException
    {
        if (records.isEmpty())
          return null;
        Record record = records.poll();
        String filename = record.getFileName();
        File file = new File(dirname+filename);
        BufferedReader br = new BufferedReader(new FileReader(file));
        /* fix skipCount */
        int skipCount = skipCounts.get(filename);
        skipCounts.put(filename, skipCount+1);
        /* skip first fileCount lines */
        for (int i = 0; i < skipCount; i++)
            br.readLine();
        String line = br.readLine();
        if (line != null) {
            String[] words = line.split(" ");
            /* HARD CODE TYPE TO BE TEXTWRITABLE */
            TextWritable key = new TextWritable();
            TextWritable value = new TextWritable();
            key.setVal(words[0]);
            value.setVal(words[1]);
            /* reinsert entry */
            records.add(new Record(key, value, filename));
            Collections.sort(records);
        }
        br.close();
        return record;
    }
}
