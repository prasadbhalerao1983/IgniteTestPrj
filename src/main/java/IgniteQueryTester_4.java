import com.data.DefaultDataAffinityKey;
import com.data.IpV4AssetGroupData;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.jetbrains.annotations.NotNull;

/**
 * Test Results generated on laptop
 *
 * Cache Name=IPV4_ASSET_GROUP_DETAIL_CACHE :: Cache Size=1_354_977
 *
 * getAffectedIPRange_1 :: SQL_1=SELECT ipv4agd.id, ipv4agd.assetGroupId, ipv4agd.ipStart, ipv4agd.ipEnd FROM IpV4AssetGroupData ipv4agd WHERE subscriptionId = ? AND assetGroupId = ? AND (ipStart <= ? AND ipEnd >= ?) ORDER BY ipv4agd.assetGroupId
 getAffectedIPRange_1 :: TimeTakenToComplete=1013 :: Size=2

 getAffectedIPRange_2 :: SQL_2=SELECT ipv4agd.id, ipv4agd.assetGroupId, ipv4agd.ipStart, ipv4agd.ipEnd FROM IpV4AssetGroupData ipv4agd JOIN TABLE (assetGroupId bigint = ? ) temp ON ipv4agd.assetGroupId = temp.assetGroupId WHERE subscriptionId = ? AND (ipStart <= ? AND ipEnd >= ?) ORDER BY ipv4agd.assetGroupId
 getAffectedIPRange_2 :: TimeTakenToComplete=1848 :: Size=2

 getAffectedIPRange_2 :: SQL_2=SELECT ipv4agd.id, ipv4agd.assetGroupId, ipv4agd.ipStart, ipv4agd.ipEnd FROM IpV4AssetGroupData ipv4agd JOIN TABLE (assetGroupId bigint = ? ) temp ON ipv4agd.assetGroupId = temp.assetGroupId WHERE subscriptionId = ? AND (ipStart <= ? AND ipEnd >= ?) ORDER BY ipv4agd.assetGroupId
 getAffectedIPRange_2 :: TimeTakenToComplete=15857 :: Size=1260

 getAffectedIPRange_3 :: SQL_3=SELECT ipv4agd.id, ipv4agd.assetGroupId, ipv4agd.ipStart, ipv4agd.ipEnd FROM IpV4AssetGroupData ipv4agd JOIN TABLE (assetGroupId bigint = ? ) temp ON ipv4agd.assetGroupId = temp.assetGroupId WHERE subscriptionId = ? ORDER BY ipv4agd.assetGroupId
 getAffectedIPRange_3 :: TimeTakenToComplete=20931 :: Size=1295
 */
public class IgniteQueryTester_4 {

  private static final String IPV4_ASSET_GROUP_DETAIL_CACHE = "IPV4_ASSET_GROUP_DETAIL_CACHE";


  private static final String SQL_1 =
      "SELECT ipv4agd.id, ipv4agd.assetGroupId, ipv4agd.ipStart, ipv4agd.ipEnd "
          + "FROM IpV4AssetGroupData ipv4agd "
          + "WHERE "
          + "subscriptionId = ? AND assetGroupId = ? AND (ipStart <= ? AND ipEnd >= ?) "
          + "ORDER BY ipv4agd.assetGroupId";

  private static final String SQL_2 =
      "SELECT ipv4agd.id, ipv4agd.assetGroupId, ipv4agd.ipStart, ipv4agd.ipEnd "
          + "FROM IpV4AssetGroupData ipv4agd "
          + "JOIN TABLE (assetGroupId bigint = ? ) temp ON ipv4agd.assetGroupId = temp.assetGroupId "
          + "WHERE "
          + "subscriptionId = ? AND (ipStart <= ? AND ipEnd >= ?) "
          + "ORDER BY ipv4agd.assetGroupId";



  private static final String SQL_3 =
      "SELECT ipv4agd.id, ipv4agd.assetGroupId, ipv4agd.ipStart, ipv4agd.ipEnd "
          + "FROM TABLE (assetGroupId bigint = ? ) temp "
          + "JOIN IpV4AssetGroupData ipv4agd ON ipv4agd.assetGroupId = temp.assetGroupId "
          + "WHERE "
          + "subscriptionId = ? "
          + "ORDER BY ipv4agd.assetGroupId";


  private Ignite ignite;


  public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {

    IgniteQueryTester_4 tester = new IgniteQueryTester_4();
    tester.init();
    tester.loadData();

    int min2=-1978662908;
    int max2=1484798802;

    long[] agIdArr1 = {3286712};
    List<Long> agIdList1 = Arrays.stream(agIdArr1).boxed().collect(Collectors.toList());

    long[] agIdArr2 = {3286712, 3286710, 3221177, 3417787, 3221180, 3319485, 3417789, 3319487, 3286720, 3221185,
        3286716, 3319490, 3319492, 3417791, 3221189, 3221191, 3319496, 3286729, 3483317, 3483324, 3417804, 3221197,
        3319481, 3286725, 3483308, 3286727, 3319478, 3417797, 3417813, 3319510, 3417815, 3417817, 3417818, 3221211,
        3319495, 3385053, 3385054, 3286751, 3483360, 3286753, 3385058, 3221219, 3483364, 3286757, 3417829, 3221222,
        3417778, 3221228, 3221229, 3385068};

    List<Long> agIdList2 = Arrays.stream(agIdArr2).boxed().collect(Collectors.toList());

    int moduleId = 1;
    long subscriptionId = 100;

    //With single agId
    tester.getAffectedIPRange_1(123,3286712,min2,max2);

    //With single agId in List ... temp join
    tester.getAffectedIPRange_2(123,agIdList1,min2,max2);

    //With 50 agId in List ... temp join
    tester.getAffectedIPRange_2(123,agIdList2,min2,max2);

    //With 50 agId in List ... temp join and without min max filter
    tester.getAffectedIPRange_3(123,agIdList2);
  }

  private void getAffectedIPRange_1(long subscriptionId, long agId, int minIp, int maxIp) {

    long start = System.currentTimeMillis();

    final IgniteCache<Object, Object> cache = ignite.cache(IPV4_ASSET_GROUP_DETAIL_CACHE);

    SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery(SQL_1);
    System.out.println("\ngetAffectedIPRange_1 :: SQL_1=" + SQL_1);
    Object[] inputArr = {subscriptionId, agId, maxIp, minIp};
    sqlFieldsQuery.setArgs(inputArr);

    cache.query(sqlFieldsQuery);

    List<Facts> dataList = getDataList(cache, sqlFieldsQuery);
    System.out.println(
        "getAffectedIPRange_1 :: TimeTakenToComplete=" + (System.currentTimeMillis() - start) + " :: Size=" + dataList
            .size());
  }

  private void getAffectedIPRange_2(long subscriptionId, List<Long> agId, int minIp, int maxIp) {
    long start = System.currentTimeMillis();

    final IgniteCache<Object, Object> cache = ignite.cache(IPV4_ASSET_GROUP_DETAIL_CACHE);

    SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery(SQL_2);
    System.out.println("\ngetAffectedIPRange_2 :: SQL_2=" + SQL_2);
    Object[] inputArr = {agId.toArray(), subscriptionId, maxIp, minIp};
    sqlFieldsQuery.setArgs(inputArr);

    cache.query(sqlFieldsQuery);

    List<Facts> dataList = getDataList(cache, sqlFieldsQuery);

    System.out.println(
        "getAffectedIPRange_2 :: TimeTakenToComplete=" + (System.currentTimeMillis() - start) + " :: Size=" + dataList
            .size());
  }

  private void getAffectedIPRange_3(long subscriptionId, List<Long> agId) {
    long start = System.currentTimeMillis();

    final IgniteCache<Object, Object> cache = ignite.cache(IPV4_ASSET_GROUP_DETAIL_CACHE);

    SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery(SQL_3);
    System.out.println("\ngetAffectedIPRange_3 :: SQL_3=" + SQL_3);
    Object[] inputArr = {agId.toArray(), subscriptionId};
    sqlFieldsQuery.setArgs(inputArr);
    sqlFieldsQuery.setEnforceJoinOrder(true);

    cache.query(sqlFieldsQuery);

    List<Facts> dataList = getDataList(cache, sqlFieldsQuery);

    System.out.println(
        "getAffectedIPRange_3 :: TimeTakenToComplete=" + (System.currentTimeMillis() - start) + " :: Size=" + dataList
            .size());
  }


  @NotNull
  private List<Facts> getDataList(IgniteCache<Object, Object> cache, SqlFieldsQuery sqlFieldsQuery) {
    List<Facts> dataList = new ArrayList<>();
    try (FieldsQueryCursor<List<?>> fieldResultsByCriteria = cache.query(sqlFieldsQuery)) {
      for (List<?> list : fieldResultsByCriteria) {

        Long id = (Long) list.get(0);
        Long assetGroupId = (Long) list.get(1);
        Integer startIp = (Integer) list.get(2);
        Integer endIp = (Integer) list.get(3);
        Facts fact = new Facts(id, assetGroupId, startIp, endIp);
        dataList.add(fact);
      }
    }
    return dataList;
  }

  private List<Long> getAssetGroupIds() {

    long[] assetGroupIdArr1 = {3286712};

    long[] assetGroupIdArr2 = {3286712, 3286710, 3221177, 3417787, 3221180, 3319485, 3417789, 3319487, 3286720, 3221185,
        3286716, 3319490, 3319492, 3417791, 3221189, 3221191, 3319496, 3286729, 3483317, 3483324, 3417804, 3221197,
        3319481, 3286725, 3483308, 3286727, 3319478, 3417797, 3417813, 3319510, 3417815, 3417817, 3417818, 3221211,
        3319495, 3385053, 3385054, 3286751, 3483360, 3286753, 3385058, 3221219, 3483364, 3286757, 3417829, 3221222,
        3417778, 3221228, 3221229, 3385068};

    List<Long> list = Arrays.stream(assetGroupIdArr2).boxed().collect(Collectors.toList());

    return list;
  }

  private void loadData() throws IOException {


    System.out.println("Curr Dir :"+Paths.get(".").toAbsolutePath().normalize().toString());

    System.out.println("Data generation started...");
    long start = System.currentTimeMillis();

    final IgniteCache<Object, Object> cache = ignite.cache(IPV4_ASSET_GROUP_DETAIL_CACHE);

    File f = new File("./src/main/resources/DummyData");
    try (BufferedReader b = new BufferedReader(new FileReader(f))) {

      String str = "";

      while ((str = b.readLine()) != null) {

        String[] tokens = str.split("\\|");

        final IpV4AssetGroupData data = createData(tokens);

        cache.put(data.getKey(), data);
      }
    }

    System.out.println("Cache Name=" + IPV4_ASSET_GROUP_DETAIL_CACHE + " :: size=" + cache.size());
    System.out.println("Data generation completed in=" + (System.currentTimeMillis() - start));
  }

  private IpV4AssetGroupData createData(String[] tokens) {
    long id = Long.parseLong(tokens[0]);
    long assetGroupId = Long.parseLong(tokens[1]);
    long subscriptionId = Long.parseLong(tokens[2]);
    int startIp = Integer.parseInt(tokens[3]);
    int endIp = Integer.parseInt(tokens[4]);
    int partitionId = Integer.parseInt(tokens[5]);
    long updatedDate = Long.parseLong(tokens[6]);
    boolean updated = Boolean.parseBoolean(tokens[7]);

    IpV4AssetGroupData data = new IpV4AssetGroupData();
    data.setId(id);
    data.setAssetGroupId(assetGroupId);
    data.setSubscriptionId(subscriptionId);
    data.setIpStart(startIp);
    data.setIpEnd(endIp);
    data.setPartitionId(partitionId);
    data.setUpdatedDate(updatedDate);
    data.setUpdated(updated);

    return data;
  }

  private void init() {
    this.ignite = Ignition.start(getIgniteConfiguration());
    this.ignite.cluster().active(true);
    System.out.println("Cluster set actviee");
  }

  private IgniteConfiguration getIgniteConfiguration() {

    TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
    String[] hosts = {"localhost"};
    ipFinder.setAddresses(Arrays.asList(hosts));

    TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
    discoSpi.setIpFinder(ipFinder);

    IgniteConfiguration cfg = new IgniteConfiguration();
    cfg.setDiscoverySpi(discoSpi);
    cfg.setIgniteInstanceName("springDataNode");
    cfg.setPeerClassLoadingEnabled(false);
    cfg.setRebalanceThreadPoolSize(4);
    //cfg.setClientMode(true);
    //  cfg.setTransactionConfiguration(transactionConfiguration());

    DataStorageConfiguration storageCfg = new DataStorageConfiguration();
    DataRegionConfiguration regionConfiguration = new DataRegionConfiguration();
    regionConfiguration.setInitialSize(3L * 1024 * 1024 * 1024);
    regionConfiguration.setMaxSize(3L * 1024 * 1024 * 1024);
    regionConfiguration.setMetricsEnabled(true);

    storageCfg.setDefaultDataRegionConfiguration(regionConfiguration);
    //storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);
    storageCfg.setStoragePath("c:/ignite-storage/storage");
    storageCfg.setWalPath("c:/ignite-storage/storage/wal");
    storageCfg.setWalArchivePath("c:/ignite-storage/storage/wal-archive");
    storageCfg.setMetricsEnabled(true);
    cfg.setDataStorageConfiguration(storageCfg);

    cfg.setCacheConfiguration(ipv4AssetGroupDetailCacheCfg());
    return cfg;
  }


  private CacheConfiguration ipv4AssetGroupDetailCacheCfg() {

    CacheConfiguration ipv4AssetGroupDetailCacheCfg = new CacheConfiguration<>(IPV4_ASSET_GROUP_DETAIL_CACHE);
    ipv4AssetGroupDetailCacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
    ipv4AssetGroupDetailCacheCfg.setWriteThrough(false);
    ipv4AssetGroupDetailCacheCfg.setReadThrough(false);
    ipv4AssetGroupDetailCacheCfg.setRebalanceMode(CacheRebalanceMode.ASYNC);
    ipv4AssetGroupDetailCacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
    ipv4AssetGroupDetailCacheCfg.setBackups(1);
    ipv4AssetGroupDetailCacheCfg.setIndexedTypes(DefaultDataAffinityKey.class, IpV4AssetGroupData.class);

    ipv4AssetGroupDetailCacheCfg.setSqlIndexMaxInlineSize(100);

    RendezvousAffinityFunction affinityFunction = new RendezvousAffinityFunction();
    affinityFunction.setExcludeNeighbors(true);
    ipv4AssetGroupDetailCacheCfg.setAffinity(affinityFunction);
    ipv4AssetGroupDetailCacheCfg.setStatisticsEnabled(true);

    return ipv4AssetGroupDetailCacheCfg;
  }

  public static class Facts {

    Long id;
    Long assetGroupId;
    Integer minIp;
    Integer maxIp;

    public Facts(Long id, Long assetGroupId, Integer minIp, Integer maxIp) {
      this.id = id;
      this.assetGroupId = assetGroupId;
      this.minIp = minIp;
      this.maxIp = maxIp;
    }

    public Long getId() {
      return id;
    }

    public Long getAssetGroupId() {
      return assetGroupId;
    }

    public Integer getMinIp() {
      return minIp;
    }

    public Integer getMaxIp() {
      return maxIp;
    }
  }

}
