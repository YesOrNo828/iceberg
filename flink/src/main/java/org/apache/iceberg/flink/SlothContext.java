package org.apache.iceberg.flink;

/**
 * Created by yexianxun@corp.netease.com on 2020/4/29.
 */
public class SlothContext {
  /**
   * kerberos登录的principal
   */
  private static String krbPrincipal;

  public static String getKrbPrincipal() {
    return krbPrincipal;
  }

  public static void setKrbPrincipal(String krbPrincipal) {
    SlothContext.krbPrincipal = krbPrincipal;
  }
}
