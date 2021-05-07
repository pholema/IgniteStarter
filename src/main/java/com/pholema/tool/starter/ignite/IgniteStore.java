package com.pholema.tool.starter.ignite;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import javax.cache.configuration.FactoryBuilder;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.log4j.Logger;

public class IgniteStore {

	private final static Logger logger = Logger.getLogger(IgniteStore.class);

	private static String ignite_vm_addresses;
	private static String ignite_vm_isClient;
	private static String ignite_communicationSpi_local_port;
	private static String ignite_vm_local_port;
	private static String ignite_vm_local_port_range;
	private static String ignite_vm_multicast_port;
	private static String ignite_rest_jetty_port;

	static {
		ignite_vm_addresses = System.getProperty("ignite.vm.addresses");
		ignite_vm_isClient = System.getProperty("ignite.vm.isClient");
		ignite_communicationSpi_local_port = System.getProperty("ignite.communicationSpi.local.port");
		ignite_vm_local_port = System.getProperty("ignite.vm.local.port");
		ignite_vm_local_port_range = System.getProperty("ignite.vm.local.port.range");
		ignite_vm_multicast_port = System.getProperty("ignite.vm.multicast.port");
		ignite_rest_jetty_port = System.getProperty("ignite.rest.jetty.port");
	}

	protected Ignite ignite;

	public Ignite getIgnite() {
		return ignite;
	}

	public <k, v> IgniteCache<k, v> getCache(String cacheName) {
		// ignite.cacheNames().forEach(f -> System.out.println(f));
		// System.out.println("cache " + ignite.cache(cacheName));
		if (ignite == null || ignite.cache(cacheName) == null) {
			throw new NoSuchElementException();
		}
		return ignite.cache(cacheName);
	}

	public <K, V> IgniteCache<K, V> getCache(String cacheName, Class<K> kClass, Class<V> vClass, CacheMode cacheMode) {
		return getCacheWithExpiryPolicy(init_cache(cacheName, kClass, vClass, cacheMode), null, 0L);
	}

	public <K, V> IgniteCache<K, V> getCache(String cacheName, Class<K> kClass, Class<V> vClass, CacheMode cacheMode,
			long durationAmount) {
		return getCacheWithExpiryPolicy(init_cache(cacheName, kClass, vClass, cacheMode), null, durationAmount);
	}

	public <K, V> IgniteCache<K, V> getCache(String cacheName, Class<K> kClass, Class<V> vClass, CacheMode cacheMode,
			TimeUnit timeUnit, long durationAmount) {
		return getCacheWithExpiryPolicy(init_cache(cacheName, kClass, vClass, cacheMode), timeUnit, durationAmount);
	}

	public <K, V> IgniteCache<K, V> getCacheWithEntity(String cacheName, Class<K> kClass, Class<V> vClass,
			CacheMode cacheMode, String cacheStoreClassName, List<QueryIndex> indexes,
			LinkedHashMap<String, String> fields, long durationAmount) {
		return getCacheWithExpiryPolicy(
				init_cache(cacheName, kClass, vClass, cacheMode, cacheStoreClassName, indexes, fields), null,
				durationAmount);
	}

	@SuppressWarnings("unchecked")
	public <K, V> IgniteCache<K, V> getCacheWithAffinityKey(String cacheName, Class<V> vClass, CacheMode cacheMode,
			long durationAmount) {
		return (IgniteCache<K, V>) getCacheWithExpiryPolicy(init_cache(cacheName, vClass, cacheMode), null,
				durationAmount);
	}

	public <K, V> IgniteCache<K, V> getCacheWithExpiryPolicy(CacheConfiguration<K, V> conf, TimeUnit timeUnit,
			Long durationAmount) {
		setExpiryPolicyFactory(conf, timeUnit, durationAmount);
		return ignite.getOrCreateCache(conf);
	}

	/**
	 * ignite start init, config from javaCode+.properties
	 */
	public void init(LifecycleBean... lifecycleBean) {
		if (this.ignite == null) {
			if (!Boolean.valueOf(ignite_vm_isClient) && ignite_rest_jetty_port != null
					&& !ignite_rest_jetty_port.isEmpty()) {
				System.setProperty("IGNITE_JETTY_PORT", ignite_rest_jetty_port);
			}
			this.ignite = Ignition
					.start(init_ignite(ignite_vm_addresses, Boolean.valueOf(ignite_vm_isClient), lifecycleBean));
		} else {
			this.ignite = Ignition.ignite();
		}
	}

	/**
	 * ignite start init
	 */
	public void init(String igniteConf) {
		if (this.ignite == null) {
			logger.info("ignite.conf:" + igniteConf);
			this.ignite = Ignition.start(igniteConf);
		} else {
			this.ignite = Ignition.ignite();
		}
		// logger.info("======Ignition.state:" + Ignition.state());
	}

	/**
	 * destroy Ignite
	 */
	public void destroy() {
		ignite.close();
	}

	/**
	 * Configure ignite using multicast.
	 */
	public static IgniteConfiguration init_ignite(String vmAddr, boolean isClient, LifecycleBean... lifecycleBeans) {
		IgniteConfiguration cfg = new IgniteConfiguration();
		TcpDiscoverySpi spi = new TcpDiscoverySpi();
		spi.setNetworkTimeout(30000);
		spi.setAckTimeout(30000);
		spi.setSocketTimeout(30000);
		//
		TcpCommunicationSpi tcpCommunicationSpi = new TcpCommunicationSpi();
		tcpCommunicationSpi.setLocalPort(Integer.parseInt(ignite_communicationSpi_local_port));
		cfg.setCommunicationSpi(tcpCommunicationSpi);
		//
		if (vmAddr == null) {
		} else {
			if (!isClient) {
				spi.setLocalPort(Integer.parseInt(ignite_vm_local_port));
				spi.setLocalPortRange(Integer.parseInt(ignite_vm_local_port_range));
			}
			//
			TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
			ipFinder.setAddresses(Arrays.asList(vmAddr.split(",")));
			spi.setIpFinder(ipFinder);
		}
		cfg.setDiscoverySpi(spi);
		cfg.setClientMode(isClient);
//		cfg.setGridName("grid_" + SystemUtil.getAppName());
		if (lifecycleBeans != null) {
			cfg.setLifecycleBeans(lifecycleBeans);
		}
		return cfg;
	}

	protected <K, V> void setExpiryPolicyFactory(CacheConfiguration<K, V> conf, TimeUnit timeUnit,
			Long durationAmount) {
		Duration duration = Duration.ETERNAL;
		if (timeUnit == null) {
			timeUnit = TimeUnit.HOURS;
		}
		if (durationAmount != 0) {
			duration = new Duration(timeUnit, durationAmount);
		}
		conf.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(duration));
	}

	protected <K, V> CacheConfiguration<K, V> init_cache(String cacheName, Class<K> kClass, Class<V> vClass,
			CacheMode cacheMode) {
		CacheConfiguration<K, V> cfg = new CacheConfiguration<>(cacheName);
		cfg.setAtomicityMode(org.apache.ignite.cache.CacheAtomicityMode.ATOMIC);
		cfg.setCacheMode(cacheMode);
		cfg.setAtomicWriteOrderMode(org.apache.ignite.cache.CacheAtomicWriteOrderMode.CLOCK);
		cfg.setMemoryMode(org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED);
		cfg.setOffHeapMaxMemory(20 * 1024L * 1024L * 1024L);
		// cfg.setStartSize(100 * 1024 * 1024);
		cfg.setIndexedTypes(kClass, vClass);
		cfg.setSqlSchema(cacheName);
		return cfg;
	}

	/**
	 * Configure cache with entity.
	 */
	protected <K, V> CacheConfiguration<K, V> init_cache(String cacheName, Class<K> kClass, Class<V> vClass,
			CacheMode cacheMode, String clzStore, List<QueryIndex> indexes, LinkedHashMap<String, String> fields) {
		CacheConfiguration<K, V> cfg = new CacheConfiguration<>(cacheName);
		cfg.setAtomicityMode(org.apache.ignite.cache.CacheAtomicityMode.ATOMIC);
		cfg.setCacheMode(cacheMode);
		cfg.setReadThrough(true);
		cfg.setWriteThrough(true);
		cfg.setCacheStoreFactory(FactoryBuilder.<CacheStore<? super K, ? super V>>factoryOf(clzStore));
		// ========================================================================================
		QueryEntity queryEntity = new QueryEntity(kClass.getName(), vClass.getName());
		queryEntity.setFields(fields);
		queryEntity.setIndexes(indexes);
		cfg.setQueryEntities(Collections.singletonList(queryEntity));
		cfg.setSqlSchema(cacheName);
		return cfg;
	}

	/**
	 * Configure cache with AffinityKey.
	 */
	protected <V> CacheConfiguration<AffinityKey<Long>, V> init_cache(String cacheName, Class<V> vClass,
			CacheMode cacheMode) {
		CacheConfiguration<AffinityKey<Long>, V> cfg = new CacheConfiguration<>(cacheName);
		cfg.setAtomicityMode(org.apache.ignite.cache.CacheAtomicityMode.ATOMIC);
		cfg.setCacheMode(cacheMode);
		cfg.setAtomicWriteOrderMode(org.apache.ignite.cache.CacheAtomicWriteOrderMode.CLOCK);
		cfg.setMemoryMode(org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED);
		cfg.setOffHeapMaxMemory(20 * 1024L * 1024L * 1024L);
		cfg.setIndexedTypes(AffinityKey.class, vClass);
		cfg.setSqlSchema(cacheName);
		return cfg;
	}

}
