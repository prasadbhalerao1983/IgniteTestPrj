package com.util;

import org.junit.Assert;
import org.junit.Test;


public class IPv4UtilTest {

  IPv4Util iPv4Util;


  @Test
  public void testAllIpV4Ranges() {
    String nextIp = "0.0.0.0";
    String ipAfterConvert;
    int ipBeforeConvert = Integer.MIN_VALUE;
    while (true) {
      int intValue = IPv4Util.ipToInt(nextIp);

      Assert.assertEquals(ipBeforeConvert, intValue);
      ipBeforeConvert++;

      //   System.out.println(intValue);

      ipAfterConvert = IPv4Util.intToIp(intValue);
      //    System.out.println(ipAfterConvert);

      Assert.assertEquals(nextIp, ipAfterConvert);
      if (nextIp.equals("255.255.255.255")) {
        break;
      }
      nextIp = nextIpAddress(nextIp);
    }

    Assert.assertEquals("1.2.3.5", nextIpAddress("1.2.3.4"));
    Assert.assertEquals("1.2.4.0", nextIpAddress("1.2.3.255"));
  }

  @Test
  public void testNextIpAddress1() {
    int nextIp = -2147483648;
    String ipInString;
    long ipAfterConvert;
    while (true) {
      ipInString = IPv4Util.intToIp(nextIp);

      ipAfterConvert = IPv4Util.ipToInt(ipInString);
      //    System.out.println(ipInString);
      Assert.assertEquals(nextIp, ipAfterConvert);
      if (nextIp == 2147483647) {
        break;
      }
      nextIp++;
    }

  }


  @Test
  public void testIps() {
    int ipInInt = -2147483648;
    String ip = IPv4Util.intToIp(ipInInt);
    Assert.assertEquals(ip, "0.0.0.0");

    ipInInt = -2147483647;
    ip = IPv4Util.intToIp(ipInInt);
    Assert.assertEquals(ip, "0.0.0.1");

    ipInInt = 0;
    ip = IPv4Util.intToIp(ipInInt);
    Assert.assertEquals(ip, "128.0.0.0");

    ipInInt = 2147483646;
    ip = IPv4Util.intToIp(ipInInt);
    Assert.assertEquals(ip, "255.255.255.254");

    ipInInt = 2147483647;
    ip = IPv4Util.intToIp(ipInInt);
    Assert.assertEquals(ip, "255.255.255.255");

  }

  @Test
  public void testIps1() {

    String ip = "0.0.0.0";
    long ipInInt = IPv4Util.ipToInt(ip);
    Assert.assertEquals(ipInInt, -2147483648);

    ip = "0.0.0.1";
    ipInInt = IPv4Util.ipToInt(ip);
    Assert.assertEquals(ipInInt, -2147483647);

    ip = "128.0.0.0";
    ipInInt = IPv4Util.ipToInt(ip);
    Assert.assertEquals(ipInInt, 0);

    ip = "255.255.255.254";
    ipInInt = IPv4Util.ipToInt(ip);
    Assert.assertEquals(ipInInt, 2147483646);

    ip = "255.255.255.255";
    ipInInt = IPv4Util.ipToInt(ip);
    Assert.assertEquals(ipInInt, 2147483647);


  }

  /**
   *
   * @param input
   * @return next ip address in string format
   */
  private String nextIpAddress(final String input) {
    final String[] tokens = input.split("\\.");
    if (tokens.length != 4) {
      throw new IllegalArgumentException();
    }
    for (int i = tokens.length - 1; i >= 0; i--) {
      final int item = Integer.parseInt(tokens[i]);
      if (item < 255) {
        tokens[i] = String.valueOf(item + 1);
        for (int j = i + 1; j < 4; j++) {
          tokens[j] = "0";
        }
        break;
      }
    }
    return new StringBuilder().append(tokens[0]).append('.').append(tokens[1]).append('.').append(tokens[2]).append('.')
        .append(tokens[3]).toString();
  }


}