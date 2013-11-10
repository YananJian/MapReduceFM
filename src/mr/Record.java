package mr;

import mr.io.Writable;

public class Record implements Comparable<Record> {
  private Writable key;
  private Writable value;
  private String filename;

  public Record(Writable key, Writable value, String filename)
  {
    this.key = key;
    this.value = value;
    this.filename = filename;
  }

  public Writable getKey()
    { return this.key; }

  public Writable getValue()
    { return this.value; }

  public String getFileName()
    { return this.filename; }

  @Override
  public int compareTo(Record other)
    { return key.getVal().hashCode() - other.key.getVal().hashCode(); }
}
