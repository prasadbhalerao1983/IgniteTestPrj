package com.data;

public interface Data<K extends DataKey> {

  public K getKey();

  public String getCacheName();
}
