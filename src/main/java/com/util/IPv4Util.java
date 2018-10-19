package com.util;


import com.util.exception.IPv4Exception;

public class IPv4Util {

  // This is the middle of the unsigned 32-bit integer range
  static final long IP2INT_OFFSET_BY_VALUE = 2147483648L;

  /**
   * Integer to IP string
   *
   * @param long ipInt
   * @return String
   */
  public static String int2ip(long ipInt) {
    ipInt += IP2INT_OFFSET_BY_VALUE;
    return long2ip_2(ipInt);
  }

  public static long intTolong(long ipInt) {
    return ipInt += IP2INT_OFFSET_BY_VALUE;
  }

  /**
   * IPv4 to Long
   *
   * @param String ipStr
   * @return long
   */
  public static long ip2int(String ipStr) throws IPv4Exception {

    long n = ip2long_2(ipStr);

    if (!long2ip_2(n).equals(ipStr)) {
      throw new IPv4Exception("Invalid IP String given " + ipStr);
    }

    if (n < 0L) {
      // 255.255.255.255 is a special case:
      n += IP2INT_OFFSET_BY_VALUE;
    } else {
      n -= IP2INT_OFFSET_BY_VALUE;
    }

    return n;
  }


  /**
   * is loop-back address ?
   *
   * @param long ipInt
   * @return boolean
   */
  public static boolean isLoopback(long ipInt) {
    return (ipInt & ((int) 0xff000000)) == ((int) 0xff000000);
  }

  /**
   * is loop-back address ?
   *
   * @param long ipInt
   * @return boolean
   */
  public static boolean isLoopback(String ipStr) {
    try {
      return isLoopback(ip2int(ipStr));
    } catch (Exception e) {
      return false;
    }
  }

  private static String long2ip_2(long ipInt) {
    return ((ipInt >> 24) & 0xFF) + "." + ((ipInt >> 16) & 0xFF) + "." + ((ipInt >> 8) & 0xFF) + "." + (ipInt & 0xFF);
    //    return int2ip(ipInt);
  }

  /**
   * Integer to IP string
   *
   * @param long ipInt
   * @return String
   */
  public static String long2ip(long ipInt) {
    //    return ((ipInt >> 24) & 0xFF) + "." + ((ipInt >> 16) & 0xFF) + "." + ((ipInt >> 8) & 0xFF) + "." + (ipInt & 0xFF);
    return int2ip(ipInt);
  }

  /**
   * IPv4 to Long
   *
   * @param String ipStr
   * @return long
   */
  public static long ip2long(String ipStr) throws IPv4Exception {
    return ip2int(ipStr);
  }


  private static long ip2long_2(String ipStr) {
    String[] addrArray = ipStr.split("\\.");

    long num = 0;
    int octet, power;

    for (int i = 0; i < addrArray.length; i++) {
      power = 3 - i;
      octet = Integer.parseInt(addrArray[i]);

      if (octet > 255 || octet < 0) {
        throw new IPv4Exception("Invalid IP String given " + ipStr);
      }

      num += (octet % 256 * Math.pow(256, power));
    }
    return num;

  }

}
