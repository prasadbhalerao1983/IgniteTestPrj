package com.data;

import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class DefaultDataAffinityKey implements DataKey<Long> {

  private static final long serialVersionUID = -6858038049181201872L;
  private long id;

  @QuerySqlField
  @AffinityKeyMapped
  private long affinityId;

  public DefaultDataAffinityKey(long id, long affinityId) {
    this.id = id;
    this.affinityId = affinityId;
  }

  @Override
  public Long getId() {
    return this.id;
  }

  public long getAffinityId() {
    return affinityId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DefaultDataAffinityKey)) {
      return false;
    }

    DefaultDataAffinityKey that = (DefaultDataAffinityKey) o;

    if (id != that.id) {
      return false;
    }
    return affinityId == that.affinityId;
  }

  @Override
  public int hashCode() {
    int result = (int) (getId() ^ (getId() >>> 32));
    result = 31 * result + (int) (affinityId ^ (affinityId >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "DefaultDataAffinityKey{" + "id=" + id + ", affinityId=" + affinityId + '}';
  }
}
