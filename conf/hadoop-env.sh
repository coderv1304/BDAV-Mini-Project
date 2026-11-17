# Environment for every Hadoop daemon in the image.
# JAVA_HOME must be explicit: the daemons are started by scripts that do not inherit
# a login shell, and a missing JAVA_HOME is the single most common "cluster will not
# start" cause.
export JAVA_HOME=/opt/java/openjdk
export HADOOP_HOME=/opt/hadoop
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export HADOOP_LOG_DIR=/opt/hadoop/logs
export HADOOP_PID_DIR=/hadoop/tmp/pids
export HADOOP_HEAPSIZE_MAX=1024
export YARN_RESOURCEMANAGER_HEAPSIZE=768
export YARN_NODEMANAGER_HEAPSIZE=768

# Keep the daemons' own logging out of the way of job output.
export HADOOP_OPTS="${HADOOP_OPTS} -Dfile.encoding=UTF-8"
