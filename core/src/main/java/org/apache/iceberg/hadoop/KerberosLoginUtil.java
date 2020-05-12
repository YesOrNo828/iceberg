package org.apache.iceberg.hadoop;

import com.google.common.io.ByteStreams;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.krb5.KrbException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.regex.Matcher;

public class KerberosLoginUtil {
  private static final Logger LOG = LoggerFactory.getLogger(KerberosLoginUtil.class);

  /*** The local sloth work file name*/
  private static final String SLOTH_FILE_NAME = "slothFile";

  /**
   * The local sloth work parent file name in local env
   */
  private static final String LOCAL_ENV_SLOTH_FILE_NAME = "sloth_test_file_hive";

  /*** The local sloth work file path*/
  private String slothFilePath;

  /*** sloth file in classpath file path**/
  private static final String CLASSPATH_SLOTH_FILE = "zz_sloth/";

  public static String classPathKey = "configurationsPath";

  public static final String KERBEROS_ENABLED = "krb.enabled";
  public static final String KERBEROS_LOGIN_CORE_XML_NAME = "security.kerberos.login.core.xml.name";
  public static final String KERBEROS_LOGIN_HDFS_XML_NAME = "security.kerberos.login.hdfs.xml.name";
  public static final String KERBEROS_LOGIN_KRB_CONF_NAME = "security.kerberos.login.krb.conf.name";
  public static final String KERBEROS_LOGIN_KEYTAB_NAME = "security.kerberos.login.keytab.name";
  public static final String KERBEROS_LOGIN_PRINCIPAL = "security.kerberos.login.principal";

  public static UserGroupInformation initKerberosEnv(Configuration oldConf) throws IOException, KrbException {
    if (oldConf == null) {
      LOG.warn("conf is null.");
      return UserGroupInformation.getCurrentUser();
    }
    String krbConfName = oldConf.get(KERBEROS_LOGIN_KRB_CONF_NAME);
    String principal = oldConf.get(KERBEROS_LOGIN_PRINCIPAL);
    String keytabName = oldConf.get(KERBEROS_LOGIN_KEYTAB_NAME);
    String classpath = oldConf.get(classPathKey);

    return initKerberosEnv(oldConf, krbConfName, principal, keytabName, classpath);
  }

  private static UserGroupInformation initKerberosEnv(Configuration conf, String krbConfName,
      String principal, String keytabName, String classpath) throws IOException, KrbException {
    String confDir;
    if (StringUtils.isNotBlank(classpath)) {
      String classPathInJar = classpath;
      LOG.info("classPathKey is {}.", classPathInJar);
      confDir = copyFiles(classPathInJar, krbConfName, keytabName);
    } else {
      URL url = Util.class.getClassLoader().getResource(CLASSPATH_SLOTH_FILE);
      if (url == null) {
        throw new KrbException("cannot find yarn_ship_path");
      }
      confDir = url.getFile();
    }
//    Configuration conf = new Configuration(false);

    // todo find principal keytab
    LOG.info("dir is {}, krbConf is {}, principal is {}, keytab is {}",
        confDir, krbConfName, principal, keytabName);
    System.setProperty("java.security.krb5.conf", confDir + File.separator + krbConfName);

    conf.set("yarn.resourcemanager.principal", principal);
    // todo add resource
//    conf.addResource(new Path(confDir, coreXmlName));
//    conf.addResource(new Path(confDir, hdfsXmlName));
    // todo login and return ugi
    sun.security.krb5.Config.refresh();
    UserGroupInformation.setConfiguration(conf);
    String keytabPath = confDir + File.separator + keytabName;
    UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
    LOG.info("login user by kerberos, principal: {}, current user is: {}, login: {}",
        UserGroupInformation.getLoginUser().getUserName(),
        UserGroupInformation.getCurrentUser().getUserName(),
        UserGroupInformation.isLoginKeytabBased());
    return UserGroupInformation.getLoginUser();
  }

  /**
   * get sloth kerberos work dir
   *
   * @throws IOException
   */
  private static String initSlothWorkDir() throws IOException {
    String localDirs = System.getenv().get("LOCAL_DIRS");
    if (localDirs == null) {
      localDirs = System.getProperty("java.io.tmpdir") + File.separator + LOCAL_ENV_SLOTH_FILE_NAME;
      LOG.info("is not cluster env, will init at local path:{}.", localDirs);
      File file = new File(localDirs);
      if (!file.exists()) {
        file.mkdir();
      }
    }
    if (StringUtils.isBlank(localDirs)) {
      throw new IOException("not find LOCAL_DIRS");
    }
    String localDir = localDirs.split(",")[0];
    String slothFilePath = localDir + File.separator + SLOTH_FILE_NAME + File.separator + System.currentTimeMillis();
    File file = new File(slothFilePath);
    file.mkdirs();
    LOG.info("initSlothWorkDir success:{}.", slothFilePath);
    return slothFilePath;
  }

  /**
   * get path file name
   *
   * @param filePath
   * @return
   * @throws IOException
   */
  private static String getPathFileName(String filePath) throws IOException {
    if (StringUtils.isBlank(filePath)) {
      throw new IOException("not find file path:" + filePath);
    }
    String[] filePaths = filePath.split(Matcher.quoteReplacement(File.separator));
    return filePaths[filePaths.length - 1];
  }

  public static String copyFiles(String sourcePath, String... fileNames) throws IOException {
    Class clz = KerberosLoginUtil.class;
    String slothDistPath = initSlothWorkDir();
    for (String fileName : fileNames) {
      String source = sourcePath + File.separator + fileName;
      copyAndGetPath(clz, source, slothDistPath, getPathFileName(source));
    }
    return slothDistPath;
  }

  /**
   * copy resource from classpath to dist path and get dist path
   * @param clazz
   * @param source    jar中的相对路径
   * @param distRootPath  拷贝的目标路径
   * @param distFileName  文件名
   * @return 目标文件路径
   */
  private static String copyAndGetPath(Class clazz, String source, String distRootPath, String distFileName) {
    File dir = new File(distRootPath);
    if (!dir.exists()) {
      dir.mkdirs();
    }
    String targetPath = distRootPath + "/" + distFileName;
    File file = new File(targetPath);
    if (file.exists()) {
      return targetPath;
    }
    copyResourceToPath(clazz, source, targetPath);
    return targetPath;
  }

  /**
   * copy resource from classpath to path
   * @param clazz
   * @param resourceName
   * @param targetPath
   */
  private static void copyResourceToPath(Class clazz, String resourceName, String targetPath) {
    try (InputStream inputStream = clazz.getClassLoader().getResourceAsStream(resourceName)) {
      if (inputStream != null) {
        try (OutputStream outputStream = new FileOutputStream(targetPath)) {
          ByteStreams.copy(inputStream, outputStream);
        }
      } else {
        throw new RuntimeException("resource not found.");
      }
    } catch (Exception ex) {
      LOG.error("", ex);
      throw new RuntimeException("failed to pull resource [" + resourceName + "] out of class to dest [" + targetPath + "]. ex:" + ex.getMessage());
    }
  }
}
