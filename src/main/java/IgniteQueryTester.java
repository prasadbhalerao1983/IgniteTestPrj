import com.data.DefaultDataAffinityKey;
import com.data.IpContainerIpV4Data;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

/**
 * Cache size: 5 Million entries
 * program Output with 6 GB heap size
 *
 CASE1: TimeTakenToComplete=3831 :: Cursor Fetch Time =3731 :: Size=100000
 CASE2: TimeTakenToComplete=159518 :: Cursor Fetch Time =156079 :: Size=4999998
 CASE3: TimeTakenToComplete=3312 :: Cursor Fetch Time =3312 :: Size=1
 CASE4: TimeTakenToComplete=3248 :: Cursor Fetch Time =3248 :: Size=1
 CASE5: TimeTakenToComplete=2797 :: Cursor Fetch Time =2797 :: Size=1

 */

public class IgniteQueryTester {

  private static final String IP_CONTAINER_IPV4_CACHE = "IP_CONTAINER_IPV4_CACHE";

  private static final String AFFECTED_IP_RANGE_QUERY =
      "select id,moduleid,subscriptionid,ipEnd,ipStart,partitionid,updateddate "
          + "from  IpContainerIpV4Data where subscriptionId = ? and moduleId = ? and (ipStart <= ? and ipEnd >= ?) "
          + "union " + "select id,moduleid,subscriptionid,ipEnd,ipStart,partitionid,updateddate "
          + "from  IpContainerIpV4Data where subscriptionId = ? and moduleId = ? and (ipStart <= ? and ipEnd >= ?) "
          + "union " + "select id,moduleid,subscriptionid,ipEnd,ipStart,partitionid,updateddate "
          + "from  IpContainerIpV4Data where subscriptionId = ? and moduleId = ? and (ipStart >= ? and ipEnd <= ?) ";

  private Ignite ignite;

  public static void main(String[] args) {

    IgniteQueryTester tester = new IgniteQueryTester();
    tester.init();

    int moduleId = 1;
    long subscriptionId = 100;
    tester.generateTestData(subscriptionId, moduleId);

    System.out.println("Test data generated...");

    //CASE1:
    long startIp = -1968911489;
    long endIp = -1967711489;
    final List<IpContainerIpV4Data> affectedIPRange1 = tester
        .getAffectedIPRange(subscriptionId, moduleId, startIp, endIp);

    //CASE2:
    startIp = -1979711434;
    endIp = -1919711490;
    final List<IpContainerIpV4Data> affectedIPRange2 = tester
        .getAffectedIPRange(subscriptionId, moduleId, startIp, endIp);


    //CASE3:
    startIp = -1967711552;
    endIp = -1967711552;
    final List<IpContainerIpV4Data> affectedIPRange3 = tester
        .getAffectedIPRange(subscriptionId, moduleId, startIp, endIp);

    //CASE4:
    startIp = -1969711480;
    endIp = -1969711488;
    final List<IpContainerIpV4Data> affectedIPRange4 = tester
        .getAffectedIPRange(subscriptionId, moduleId, startIp, endIp);

    //CASE5:
    startIp = -1979711488;
    endIp = -1979711438;
    final List<IpContainerIpV4Data> affectedIPRange5 = tester
        .getAffectedIPRange(subscriptionId, moduleId, startIp, endIp);

    System.out.println("affectedIPRange5::"+affectedIPRange5.toString());

    System.out.println("Exiting.....");
    //System.exit(0);
  }

  public List<IpContainerIpV4Data> getAffectedIPRange(long subScirptionId, long containerId, long minIp, long maxIp) {
    long start = System.currentTimeMillis();
    long cursorFetchTime = 0;

    final IgniteCache<Object, Object> cache = ignite.cache(IP_CONTAINER_IPV4_CACHE);

    SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery(AFFECTED_IP_RANGE_QUERY)
        .setArgs(subScirptionId, containerId, minIp, minIp, subScirptionId, containerId, maxIp, maxIp, subScirptionId,
            containerId, minIp, maxIp);
    cache.query(sqlFieldsQuery);

    List<IpContainerIpV4Data> ipV4DataList = new ArrayList<>();
    try (FieldsQueryCursor<List<?>> fieldResultsByCriteria = cache.query(sqlFieldsQuery)) {
      for (List<?> list : fieldResultsByCriteria) {
        if (cursorFetchTime == 0) {
          cursorFetchTime = System.currentTimeMillis();
        }
        IpContainerIpV4Data ipV4Data = new IpContainerIpV4Data();
        ipV4Data.setId((Long) list.get(0));
        ipV4Data.setModuleId((Integer) list.get(1));
        ipV4Data.setSubscriptionId((Long) list.get(2));
        ipV4Data.setIpEnd((Long) list.get(3));
        ipV4Data.setIpStart((Long) list.get(4));
        ipV4Data.setPartitionId((Integer) list.get(5));
        ipV4Data.setUpdatedOn((Long) list.get(6));
        ipV4DataList.add(ipV4Data);
      }
    }
    System.out.println(
        "TimeTakenToComplete=" + (System.currentTimeMillis() - start) + " :: Cursor Fetch Time =" + (cursorFetchTime
            - start) + " :: Size=" + ipV4DataList.size());
    return ipV4DataList;
  }

  private void init() {
    this.ignite = Ignition.start(getIgniteConfiguration());
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
    regionConfiguration.setInitialSize(2L * 1024 * 1024 * 1024);
    regionConfiguration.setMaxSize(4L * 1024 * 1024 * 1024);
    storageCfg.setDefaultDataRegionConfiguration(regionConfiguration);
    //        storageCfg.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);
    //        storageCfg.setStoragePath(dataStoragePath);
    //        storageCfg.setWalPath(dataStoragePath);
    //        storageCfg.setWalArchivePath(dataStoragePath);
    cfg.setDataStorageConfiguration(storageCfg);

    cfg.setCacheConfiguration(ipContainerIPV4CacheCfg());
    return cfg;
  }

  private CacheConfiguration ipContainerIPV4CacheCfg() {

    CacheConfiguration ipContainerIpV4CacheCfg = new CacheConfiguration<>(IP_CONTAINER_IPV4_CACHE);
    ipContainerIpV4CacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
    ipContainerIpV4CacheCfg.setWriteThrough(false);
    ipContainerIpV4CacheCfg.setReadThrough(false);
    ipContainerIpV4CacheCfg.setRebalanceMode(CacheRebalanceMode.ASYNC);
    ipContainerIpV4CacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
    //ipContainerIpV4CacheCfg.setOnheapCacheEnabled(true);
    ipContainerIpV4CacheCfg.setBackups(1);
    ipContainerIpV4CacheCfg.setIndexedTypes(DefaultDataAffinityKey.class, IpContainerIpV4Data.class);
    ipContainerIpV4CacheCfg.setSqlIndexMaxInlineSize(100);
    RendezvousAffinityFunction affinityFunction = new RendezvousAffinityFunction();
    affinityFunction.setExcludeNeighbors(true);
    ipContainerIpV4CacheCfg.setAffinity(affinityFunction);

    return ipContainerIpV4CacheCfg;
  }

  private void generateTestData(long subscriptionId, int moduleId) {

    System.out.println("Data generation started...");

    final IgniteCache<Object, Object> cache = ignite.cache(IP_CONTAINER_IPV4_CACHE);

    //long start = -1979711488;
    long endIp = 0;
    long startIp = -1979711488;

    IpContainerIpV4Data data = null;

    for (int ctr = 1; ctr <= 5000_000; ) {

      endIp = startIp + 50;

      data = createData(ctr, subscriptionId, moduleId, startIp, endIp);

      cache.put(data.getKey(), data);
      ctr++;

      startIp = endIp;

      for (int i = 1; i <= 4; i++) {

        startIp = startIp + 2;
        endIp = startIp;

        data = createData(ctr, subscriptionId, moduleId, startIp, endIp);

        cache.put(data.getKey(), data);
        ctr++;
      }
      startIp = startIp + 2;


    }
    System.out.println("Final :: startIp=" + startIp + " :: endIp=" + endIp);
    System.out.println("Cache Name=" + IP_CONTAINER_IPV4_CACHE + " :: size=" + cache.size());
  }

  private IpContainerIpV4Data createData(long id, long subscriptionId, int moduleId, long startIp, long endIp) {

    IpContainerIpV4Data data = new IpContainerIpV4Data();
    data.setId(id);
    data.setSubscriptionId(subscriptionId);
    data.setModuleId(moduleId);
    data.setIpStart(startIp);
    data.setIpEnd(endIp);
    return data;
  }

}
