/*
 * Utility to adjust logging levels for this package at runtime, without depending
 * on a specific logging backend at compile time. Uses reflection to support
 * Log4j2 (preferred in Flink), and best-effort fallbacks for Logback and Log4j 1.x.
 */
package org.apache.flink.contrib.streaming.state;

final class LoggingUtils {
    private LoggingUtils() {}

    /**
     * Set the log level for a given logger/package name. Level names are case-insensitive
     * (e.g., "ERROR", "WARN", "INFO", "DEBUG"). Best-effort: silently ignores failures
     * if the logging backend is not available or cannot be configured programmatically.
     */
    @SuppressWarnings({"unchecked","rawtypes"})
    public static void setPackageLogLevel(String loggerName, String levelName) {
        if (loggerName == null || levelName == null) {
            return;
        }
        String lvl = levelName.trim().toUpperCase();
        // Try Log4j2 first (used by Flink 1.15+ by default)
        try {
            Class<?> configurator = Class.forName("org.apache.logging.log4j.core.config.Configurator");
            Class<?> levelClass = Class.forName("org.apache.logging.log4j.Level");
            Object lvlEnum = Enum.valueOf((Class<Enum>) levelClass, lvl);
            java.lang.reflect.Method m = configurator.getMethod("setLevel", String.class, levelClass);
            m.invoke(null, loggerName, lvlEnum);
            return;
        } catch (Throwable ignore) {
            // fall through
        }
        // Try Logback
        try {
            Object logger = org.slf4j.LoggerFactory.getLogger(loggerName);
            Class<?> lbLoggerClass = Class.forName("ch.qos.logback.classic.Logger");
            if (lbLoggerClass.isInstance(logger)) {
                Class<?> lbLevelClass = Class.forName("ch.qos.logback.classic.Level");
                Object lbLevel = lbLevelClass.getMethod("toLevel", String.class).invoke(null, lvl);
                lbLoggerClass.getMethod("setLevel", lbLevelClass).invoke(logger, lbLevel);
                return;
            }
        } catch (Throwable ignore) {
            // fall through
        }
        // Try Log4j 1.x
        try {
            Class<?> levelClazz = Class.forName("org.apache.log4j.Level");
            Object l = levelClazz.getMethod("toLevel", String.class).invoke(null, lvl);
            Class<?> logManager = Class.forName("org.apache.log4j.LogManager");
            Object logger = logManager.getMethod("getLogger", String.class).invoke(null, loggerName);
            logger.getClass().getMethod("setLevel", levelClazz).invoke(logger, l);
        } catch (Throwable ignore) {
            // give up silently
        }
    }
}
