import com.data.DefaultDataAffinityKey;
import com.data.IpContainerIpV4Data;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import javax.cache.configuration.Factory;
import javax.cache.configuration.FactoryBuilder;
import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
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
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

public class IgniteTransactionTester {

  private static final String IP_CONTAINER_IPV4_CACHE = "IP_CONTAINER_IPV4_CACHE";

  private static final String QUERY = "select id,moduleid,subscriptionid,ipEnd,ipStart,partitionid,updateddate from  IpContainerIpV4Data ";

  private Ignite ignite;

  public static void main(String[] args) {

    IgniteTransactionTester tester = new IgniteTransactionTester();
    tester.init();
    tester.testTransactionException();

    System.out.println("Exiting.....");
    System.exit(0);
  }


  private void testTransactionException(){


    IgniteTransactions transactions = ignite.transactions();
    try (Transaction tx = transactions.txStart(TransactionConcurrency.OPTIMISTIC, TransactionIsolation.SERIALIZABLE)) {

      final IgniteCache<Object, Object> cache = ignite.cache(IP_CONTAINER_IPV4_CACHE);
      SqlFieldsQuery sqlFieldsQuery = new SqlFieldsQuery(QUERY);
      cache.query(sqlFieldsQuery);

      List<IpContainerIpV4Data> ipV4DataList = new ArrayList<>();
      try (FieldsQueryCursor<List<?>> fieldResultsByCriteria = cache.query(sqlFieldsQuery)) {

        for (List<?> list : fieldResultsByCriteria) {

          IpContainerIpV4Data ipV4Data = new IpContainerIpV4Data();
          ipV4Data.setId((Long) list.get(0));
          ipV4Data.setModuleId((Integer) list.get(1));
          ipV4Data.setSubscriptionId((Long) list.get(2));
          ipV4Data.setIpEnd((Integer) list.get(3));
          ipV4Data.setIpStart((Integer) list.get(4));
          ipV4Data.setPartitionId((Integer) list.get(5));
          ipV4Data.setUpdatedOn((Long) list.get(6));
          ipV4DataList.add(ipV4Data);
        }
      }
    }

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
    cfg.setIgniteInstanceName("tester-node");
    cfg.setPeerClassLoadingEnabled(false);
    cfg.setRebalanceThreadPoolSize(4);

    DataStorageConfiguration storageCfg = new DataStorageConfiguration();
    DataRegionConfiguration regionConfiguration = new DataRegionConfiguration();
    regionConfiguration.setMetricsEnabled(true);
    storageCfg.setDefaultDataRegionConfiguration(regionConfiguration);
    cfg.setDataStorageConfiguration(storageCfg);

    cfg.setCacheConfiguration(ipContainerIPV4CacheCfg());
    return cfg;
  }

  private CacheConfiguration ipContainerIPV4CacheCfg() {

    CacheConfiguration ipContainerIpV4CacheCfg = new CacheConfiguration<>("IP_CONTAINER_IPV4_CACHE");
    ipContainerIpV4CacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
    ipContainerIpV4CacheCfg.setWriteThrough(false);
    ipContainerIpV4CacheCfg.setReadThrough(false);
    ipContainerIpV4CacheCfg.setRebalanceMode(CacheRebalanceMode.ASYNC);
    ipContainerIpV4CacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
    ipContainerIpV4CacheCfg.setBackups(1);
    ipContainerIpV4CacheCfg.setIndexedTypes(DefaultDataAffinityKey.class, IpContainerIpV4Data.class);
    ipContainerIpV4CacheCfg.setSqlIndexMaxInlineSize(84);
    RendezvousAffinityFunction affinityFunction = new RendezvousAffinityFunction();
    affinityFunction.setExcludeNeighbors(true);
    ipContainerIpV4CacheCfg.setAffinity(affinityFunction);
    ipContainerIpV4CacheCfg.setStatisticsEnabled(true);

    return ipContainerIpV4CacheCfg;
  }


}


