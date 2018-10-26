package com.util;


import com.util.exception.IPv4Exception;

public class IPv4Util {

  // This is the middle of the unsigned 32-bit integer range
  static final long IP2INT_OFFSET_BY_VALUE = 2147483648L;


  /**
   * integer to IP string
   */
  public static String intToIp(int ipInt) {
    ipInt += IP2INT_OFFSET_BY_VALUE;
    return intToIpConv(ipInt);
  }

  /**
   * IPv4 to integer
   */
  public static int ipToInt(String ipStr) throws IPv4Exception {

    long n = ipToLongConv(ipStr);

    if (!intToIpConv(n).equals(ipStr)) {
      throw new IPv4Exception("Invalid IP String given " + ipStr);
    }

    int intVal;
    if (n < 0L) {
      // 255.255.255.255 is a special case:
      n += IP2INT_OFFSET_BY_VALUE;
    } else {
      n -= IP2INT_OFFSET_BY_VALUE;
    }

    return (int)n;
  }

  /**
   * is loop-back address ?
   */
  public static boolean isLoopback(int ipInt) {
    return (ipInt & (0xff000000)) == (0xff000000);
  }

  /**
   * is loop-back address ?
   */
  public static boolean isLoopback(String ipStr) {
    try {
      return isLoopback(ipToInt(ipStr));
    } catch (Exception e) {
      return false;
    }
  }

  private static String intToIpConv(long ipInt) {
    return ((ipInt >> 24) & 0xFF) + "." + ((ipInt >> 16) & 0xFF) + "." + ((ipInt >> 8) & 0xFF) + "." + (ipInt & 0xFF);
  }

  private static long ipToLongConv(String ipStr) {
    String[] addrArray = ipStr.split("\\.");

    long num = 0;
    int octet;
    int power;

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
