package mr;

import java.util.LinkedList;

import mr.io.Writable;

public class Record implements Comparable<Record> {
  private Writable key;
  private Iterable<Writable> values;
  private String filename;

  public Record(Writable key, String filename)
  {
    this.key = key;
    this.values = new LinkedList<Writable>();
    this.filename = filename;
  }

  public Writable getKey()
    { return this.key; }

  public void addValue(Writable value)
    { ((LinkedList<Writable>)values).add(value); }

  public Iterable<Writable> getValues()
    { return this.values; }

  public String getFileName()
    { return this.filename; }

  @Override
  public int compareTo(Record other)
    { return new Integer(key.getVal().hashCode()).compareTo(new Integer(other.key.getVal().hashCode())); }
}
