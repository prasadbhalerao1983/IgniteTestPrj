package com.data;

import com.util.IPv4Util;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Takes approx 5 GB on disk for 15 million entries when persistence is enabled.
 *
 */
public class IpContainerIpV4Data implements Data<DefaultDataAffinityKey> {

  @QuerySqlField
  private long id;

  @QuerySqlField(orderedGroups = {@QuerySqlField.Group(name = "ip_container_ipv4_idx1", order = 2)})
  private int moduleId;
  @QuerySqlField(orderedGroups = {@QuerySqlField.Group(name = "ip_container_ipv4_idx1", order = 1),
      @QuerySqlField.Group(name = "ip_container_ipv4_idx2", order = 0)})
  private long subscriptionId;
  @QuerySqlField(orderedGroups = {@QuerySqlField.Group(name = "ip_container_ipv4_idx1", order = 4, descending = true),
      @QuerySqlField.Group(name = "ip_container_ipv4_idx2", order = 2, descending = true)})
  private int ipEnd;
  @QuerySqlField(orderedGroups = {@QuerySqlField.Group(name = "ip_container_ipv4_idx1", order = 3),
      @QuerySqlField.Group(name = "ip_container_ipv4_idx2", order = 1)})
  private int ipStart;
  @QuerySqlField
  private int partitionId;
  @QuerySqlField
  private long updatedDate;

  private boolean updated = false;

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public long getSubscriptionId() {
    return subscriptionId;
  }

  public void setSubscriptionId(long subscriptionId) {
    this.subscriptionId = subscriptionId;
  }

  public int getModuleId() {
    return moduleId;
  }

  public void setModuleId(int moduleId) {
    this.moduleId = moduleId;
  }

  public int getIpEnd() {
    return ipEnd;
  }

  public void setIpEnd(int ipEnd) {
    this.ipEnd = ipEnd;
  }

  public int getIpStart() {
    return ipStart;
  }

  public void setIpStart(int ipStart) {
    this.ipStart = ipStart;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public void setPartitionId(int partitionId) {
    this.partitionId = partitionId;
  }

  public long getUpdatedOn() {
    return updatedDate;
  }

  public void setUpdatedOn(long updatedOn) {
    this.updatedDate = updatedOn;
  }

  @Override
  public DefaultDataAffinityKey getKey() {
    return new DefaultDataAffinityKey(id, subscriptionId);
  }

  @Override
  public String getCacheName() {
    return "IP_CONTAINER_IPV4_CACHE";
  }

  //  @Override
  //  public String toString() {
  //    return "IpContainerIpV4Data{" + "id=" + id + ", moduleId=" + moduleId + ", subscriptionId=" + subscriptionId
  //        + ", ipStart=" + IPv4Util.long2ip(ipStart) + ", ipEnd=" + IPv4Util.long2ip(ipEnd) + ", partitionId="
  //        + partitionId + ", updatedDate=" + updatedDate + '}';
  //  }

  @Override
  public String toString() {
    return "IpContainerIpV4Data{" + ", ipStart=" + IPv4Util.intToIp(ipStart) + ", ipEnd=" + IPv4Util.intToIp(ipEnd)
        + '}';
  }

}
