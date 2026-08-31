FROM debian:bookworm-slim

# Install necessary packages
RUN apt-get update \
  && apt-get install -y git curl gnupg build-essential tree vim cmake postgresql-server-dev-15 libproj-dev libjson-c-dev libgsl-dev libgeos-dev postgis \
  && export GNUPGHOME="$(mktemp -d)" \
  && curl -fL https://apt.corretto.aws/corretto.key | gpg --batch --import \
  && gpg --batch --export '6DC3636DAE534049C8B94623A122542AB04F24E3' > /usr/share/keyrings/corretto.gpg \
  && rm -r "$GNUPGHOME" \
  && unset GNUPGHOME \
  && echo "deb [signed-by=/usr/share/keyrings/corretto.gpg] https://apt.corretto.aws stable main" > /etc/apt/sources.list.d/corretto.list \
  && apt-get update \
  && apt-get install -y java-21-amazon-corretto-jdk 

# Build MobilityDB with MEOS. The jar this image runs is generated from the
# catalog of MobilityDB master with every family on, so the library must answer
# for the same surface: -DALL=ON is what CMakeLists.txt turns into the family
# list, and a narrower build leaves the jar naming symbols libmeos lacks.
RUN git clone --depth 1 https://github.com/MobilityDB/MobilityDB.git -b master /usr/local/src/MobilityDB
RUN mkdir -p /usr/local/src/MobilityDB/build
RUN cd /usr/local/src/MobilityDB/build && \
    cmake -DMEOS=ON -DALL=ON .. && \
    make -j$(nproc) && \
    make install && \
    ldconfig

# Set up Maven
ENV MAVEN_HOME /usr/share/maven
ENV MAVEN_CONFIG "/root/.m2"
COPY --from=maven:3.9.6-eclipse-temurin-11 ${MAVEN_HOME} ${MAVEN_HOME}
COPY --from=maven:3.9.6-eclipse-temurin-11 /usr/local/bin/mvn-entrypoint.sh /usr/local/bin/mvn-entrypoint.sh
COPY --from=maven:3.9.6-eclipse-temurin-11 /usr/share/maven/ref/settings-docker.xml /usr/share/maven/ref/settings-docker.xml
RUN ln -s ${MAVEN_HOME}/bin/mvn /usr/bin/mvn

# Clean up
RUN rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*lists/* 
RUN apt-get remove --purge --autoremove -y curl gnupg 

# Copy project-specific files
COPY target/flink-kafka2postgres-1.0-SNAPSHOT.jar /app/flink-kafka2postgres.jar
COPY wait-for-it.sh /app/wait-for-it.sh

# Make the wait-for-it script executable
RUN chmod +x /app/wait-for-it.sh

# Set the library path
ENV LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH

# Set working directory
WORKDIR /app

# Set entrypoint with the correct classpath
ENTRYPOINT ["/app/wait-for-it.sh", "kafka:29092", "--", "java", "-cp", "/app/flink-kafka2postgres.jar", "--add-opens", "java.base/java.lang=ALL-UNNAMED", "aisdata.Query9_Main"]
