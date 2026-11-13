FROM eclipse-temurin:17.0.15_6-jdk

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl gnupg wget netcat-openbsd tar sudo inetutils-ping python3 && \
    rm -rf /var/lib/apt/lists/*

# Install the same just release on x86-64 and ARM64 development machines.
RUN set -eux; \
    case "$(uname -m)" in \
      x86_64) checksum=4a5cc2f53e6f0f8c59092a6cc38291eb729d46a7dd95d3ae582008881b84931d ;; \
      aarch64) checksum=748237128c4c40cbdabc65e841d05ceba13cc23a91eaba395495894c1d9764df ;; \
      *) echo "Unsupported architecture: $(uname -m)" >&2; exit 1 ;; \
    esac; \
    curl -fsSL "https://github.com/casey/just/releases/download/1.58.0/just-1.58.0-$(uname -m)-unknown-linux-musl.tar.gz" -o /tmp/just.tar.gz; \
    echo "$checksum  /tmp/just.tar.gz" | sha256sum -c -; \
    tar -xzf /tmp/just.tar.gz -C /usr/local/bin just; \
    rm /tmp/just.tar.gz; \
    just --version

RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/sbt_old.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo tee /etc/apt/trusted.gpg.d/sbt.asc && \
    sudo apt-get update && \
    sudo apt-get install -y sbt

COPY --from=apache/spark:4.0.0-scala2.13-java17-ubuntu /opt/spark /opt/spark

COPY --from=flink:2.0.0-scala_2.12-java17 /opt/flink /opt/flink

COPY --from=apache/kafka:4.0.0 /opt/kafka /opt/kafka

ENV PATH=$PATH:/opt/spark/bin:/opt/flink/bin:/opt/kafka/bin     

WORKDIR /workspace
