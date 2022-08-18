package org.apache.hadoop.hive.cli;

import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.thrift.TReservedWordsVersion;

public class BackendConfigUtil {
  public static void initializeBackendConfig(HiveConf conf) {
    BackendConfig.create(getBackendConfig(conf), false);
  }

  /**
   * This method is only used for test purposes. According to the given HiveConf, it
   * initializes Impala's TBackendGflags, which in turn is used to create Impala's
   * BackendConfig. Refer to common/thrift/BackendGflags.thrift under Impala's
   * repository for the currently supported TBackendGflags.
   * Properly initializing BackendConfig is necessary since we require some fields in
   * TBackendGflags to be a non-null value, e.g., 'ignored_dir_prefix_list'.
   * Specifically, we will encounter a NullPointerException when
   * BackendConfig.INSTANCE.getIgnoredDirPrefixList() returns null and we call a method
   * on this null object during the initialization of Impala's FileSystemUtil.
   * On the other hand, we have to determine the corresponding type of a field
   * before calling setFieldValue() to set up the value of the field. Otherwise, a
   * ClassCastException would be thrown.
   */
  public static TBackendGflags getBackendConfig(HiveConf conf) {
    Properties props = conf.getAllProperties();
    TBackendGflags cfg = new TBackendGflags();

    if (props.size() == 0) return cfg;

    String versionFieldName = TBackendGflags._Fields.RESERVED_WORDS_VERSION.getFieldName();
    String version =
        props.getProperty(Constants.IMPALA_PREFIX.concat(versionFieldName));
    TBackendGflags._Fields versionField =
        TBackendGflags._Fields.findByName(versionFieldName);
    if (versionField != null && version != null) {
      switch (version.toLowerCase()) {
        case "impala_2_11":
          cfg.setFieldValue(versionField, TReservedWordsVersion.IMPALA_2_11);
          break;
        case "impala_3_0":
          cfg.setFieldValue(versionField, TReservedWordsVersion.IMPALA_3_0);
          break;
        default:
          throw new RuntimeException("Value for '" + versionField.getFieldName()
              + " 'is not supported.");
      }
      // Remove the key and its corresponding value from the Hashtable so that we will
      // not encounter it when iterating over 'props.entrySet()' in the following
      // for-loop.
      props.remove(Constants.IMPALA_PREFIX.concat(versionFieldName));
    }

    for (Map.Entry<Object, Object> e: props.entrySet()) {
      String key = (String) e.getKey();

      if (key.startsWith(Constants.IMPALA_PREFIX) &&
          key.length() > Constants.IMPALA_PREFIX.length()) {
        String keyWithoutNameSpace =
            key.substring(Constants.IMPALA_PREFIX.length()).toLowerCase();
        TBackendGflags._Fields field = TBackendGflags._Fields.findByName(
            keyWithoutNameSpace.toLowerCase());
        if (field == null) continue;

        // For fields of type string, cfg.getFieldValue(field) is null. We do not have
        // to consider the case when the type is TReservedWordsVersion since that has
        // already been taken care of before the for-loop.
        if (cfg.getFieldValue(field) == null) {
          cfg.setFieldValue(field, e.getValue());
          continue;
        }

        // For fields of types bool, double, i32, and i64, cfg.getFieldValue(field) is
        // a non-null value.
        String className = cfg.getFieldValue(field).getClass().getName();
        switch (className) {
          case "java.lang.Boolean":
            cfg.setFieldValue(field, new Boolean(e.getValue().toString()));
            break;
          case "java.lang.Double":
            cfg.setFieldValue(field, new Double(e.getValue().toString()));
            break;
          case "java.lang.Integer":
            cfg.setFieldValue(field, new Integer(e.getValue().toString()));
            break;
          case "java.lang.Long":
            cfg.setFieldValue(field, new Long(e.getValue().toString()));
            break;
          default:
            throw new RuntimeException("Class " + className + " is not expected here.");
        }
      }
    }
    return cfg;
  }
}
