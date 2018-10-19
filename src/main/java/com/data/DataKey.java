package com.data;

import java.io.Serializable;

public interface DataKey<K> extends Serializable{

  K getId();
}
