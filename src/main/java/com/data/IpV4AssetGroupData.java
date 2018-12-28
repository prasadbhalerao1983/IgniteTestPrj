package com.data;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class IpV4AssetGroupData implements Data<DefaultDataAffinityKey> {

  @QuerySqlField
  private long id;

  @QuerySqlField(orderedGroups = {@QuerySqlField.Group(name = "ipv4_asset_group_data_idx2", order = 2)})
  private long assetGroupId;

  @QuerySqlField(orderedGroups = {@QuerySqlField.Group(name = "ipv4_asset_group_data_idx1", order = 1),
      @QuerySqlField.Group(name = "ipv4_asset_group_data_idx2", order = 1)})
  private long subscriptionId;

  @QuerySqlField(orderedGroups = {@QuerySqlField.Group(name = "ipv4_asset_group_data_idx1", order = 2),
      @QuerySqlField.Group(name = "ipv4_asset_group_data_idx2", order = 3)})
  private int ipStart;

  @QuerySqlField(orderedGroups = {@QuerySqlField.Group(name = "ipv4_asset_group_data_idx1", order = 3),
      @QuerySqlField.Group(name = "ipv4_asset_group_data_idx2", order = 4)})
  private int ipEnd;

  @QuerySqlField
  private int partitionId;

  private long updatedDate;

  private boolean updated = false;

  @Override
  public DefaultDataAffinityKey getKey() {
    return new DefaultDataAffinityKey(this.id, this.subscriptionId);
  }

  @Override
  public String getCacheName() {
    return "IPV4_ASSET_GROUP_DETAIL_CACHE";
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public long getAssetGroupId() {
    return assetGroupId;
  }

  public void setAssetGroupId(long assetGroupId) {
    this.assetGroupId = assetGroupId;
  }

  public long getSubscriptionId() {
    return subscriptionId;
  }

  public void setSubscriptionId(long subscriptionId) {
    this.subscriptionId = subscriptionId;
  }

  public int getIpStart() {
    return ipStart;
  }

  public void setIpStart(int ipStart) {
    this.ipStart = ipStart;
  }

  public int getIpEnd() {
    return ipEnd;
  }

  public void setIpEnd(int ipEnd) {
    this.ipEnd = ipEnd;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public void setPartitionId(int partitionId) {
    this.partitionId = partitionId;
  }

  public long getUpdatedDate() {
    return updatedDate;
  }

  public void setUpdatedDate(long updatedDate) {
    this.updatedDate = updatedDate;
  }

  public boolean isUpdated() {
    return updated;
  }

  public void setUpdated(boolean updated) {
    this.updated = updated;
  }
}
