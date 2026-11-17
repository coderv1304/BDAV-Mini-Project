# Single image for every cluster role: the difference between a NameNode and a
# DataNode is the entrypoint, not the software - which is also why the compose file
# can add workers without touching the image.
FROM eclipse-temurin:11-jdk

ARG HADOOP_VERSION=3.3.6
ENV HADOOP_VERSION=${HADOOP_VERSION}
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH=${PATH}:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl procps netcat-openbsd ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Pinned distribution: reproducibility matters more than the newest point release.
RUN curl -fsSL --retry 3 \
      "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
      -o /tmp/hadoop.tgz \
 && tar -xzf /tmp/hadoop.tgz -C /opt \
 && mv "/opt/hadoop-${HADOOP_VERSION}" "${HADOOP_HOME}" \
 && rm /tmp/hadoop.tgz

# The build context is the repository root (see docker-compose.yml), so the site
# configuration and the entrypoints are addressed by their repository paths.
COPY conf/ ${HADOOP_CONF_DIR}/
COPY docker/entrypoint-namenode.sh docker/entrypoint-datanode.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint-*.sh \
 && mkdir -p /hadoop/tmp /hadoop/name /hadoop/data \
 && hdfs namenode -format -nonInteractive -force >/dev/null 2>&1 || true

WORKDIR /opt/app

# NameNode/RM by default; the worker service overrides this with `command:`.
# This must be CMD, not ENTRYPOINT: with an ENTRYPOINT in place compose's `command:`
# is only appended as an argument, which silently made every worker container run
# the NameNode entrypoint and crash trying to bind namenode:9000 locally.
CMD ["/usr/local/bin/entrypoint-namenode.sh"]
