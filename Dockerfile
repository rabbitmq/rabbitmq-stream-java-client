FROM ubuntu:20.04 as builder

# from https://github.com/rabbitmq/rabbitmq-java-tools-binaries-dev/releases/
ARG stream_perf_test_version="set-version-here"

RUN set -eux; \
	\
	apt-get update; \
	apt-get install --yes --no-install-recommends \
		ca-certificates \
		wget \
		gnupg

ENV JAVA_VERSION="11"
ENV JAVA_SHA256="ae78aa45f84642545c01e8ef786dfd700d2226f8b12881c844d6a1f71789cb99"
ENV JAVA_URL="https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.10%2B9/OpenJDK11U-jdk_x64_linux_hotspot_11.0.10_9.tar.gz"

RUN set -eux; \
    \
    JAVA_PATH="/usr/lib/jdk-$JAVA_VERSION"; \
	\
    wget --progress dot:giga --output-document "$JAVA_PATH.tar.gz" "$JAVA_URL"; \
	echo "$JAVA_SHA256 *$JAVA_PATH.tar.gz" | sha256sum --check --strict -; \
	mkdir -p "$JAVA_PATH"; \
	tar --extract --file "$JAVA_PATH.tar.gz" --directory "$JAVA_PATH" --strip-components 1; \
	$JAVA_PATH/bin/jlink --compress=2 --output /jre --add-modules java.base,java.naming,java.xml,jdk.unsupported; \
	/jre/bin/java -version

# pgpkeys.uk is quite reliable, but allow for substitutions locally
ARG PGP_KEYSERVER=hkps://keys.openpgp.org
# If you are building this image locally and are getting `gpg: keyserver receive failed: No data` errors,
# run the build with a different PGP_KEYSERVER, e.g. docker build --tag rabbitmq:3.7 --build-arg PGP_KEYSERVER=pgpkeys.eu 3.7/ubuntu
# For context, see https://github.com/docker-library/official-images/issues/4252

# https://www.rabbitmq.com/signatures.html#importing-gpg
ENV RABBITMQ_PGP_KEY_ID="0x0A9AF2115F4687BD29803A206B73A36E6026DFCA"
ENV STREAM_PERF_TEST_HOME="/stream_perf_test"

RUN set -eux; \
    \
    STREAM_PERF_TEST_URL=https://github.com/rabbitmq/rabbitmq-java-tools-binaries-dev/releases/download/v-stream-perf-test-$stream_perf_test_version/stream-perf-test-$stream_perf_test_version.jar; \
    STREAM_PERF_TEST_PATH="/usr/local/src/stream-perf-test-$stream_perf_test_version"; \
    \
    wget --progress dot:giga --output-document "$STREAM_PERF_TEST_PATH.jar.asc" "$STREAM_PERF_TEST_URL.asc"; \
    wget --progress dot:giga --output-document "$STREAM_PERF_TEST_PATH.jar" "$STREAM_PERF_TEST_URL"; \
    STREAM_PERF_TEST_SHA256="$(wget -qO- $STREAM_PERF_TEST_URL.sha256)"; \
    echo "$STREAM_PERF_TEST_SHA256 *$STREAM_PERF_TEST_PATH.jar" | sha256sum --check --strict -; \
    \
    export GNUPGHOME="$(mktemp -d)"; \
    gpg --batch --keyserver "$PGP_KEYSERVER" --recv-keys "$RABBITMQ_PGP_KEY_ID"; \
    gpg --batch --verify "$STREAM_PERF_TEST_PATH.jar.asc" "$STREAM_PERF_TEST_PATH.jar"; \
    gpgconf --kill all; \
    rm -rf "$GNUPGHOME"; \
    \
    mkdir -p "$STREAM_PERF_TEST_HOME"; \
    cp $STREAM_PERF_TEST_PATH.jar $STREAM_PERF_TEST_HOME/stream-perf-test.jar

FROM ubuntu:20.04

# we need locales support for characters like µ to show up correctly in the console
RUN set -eux; \
	apt-get update; \
	apt-get install -y --no-install-recommends \
		locales \
	; \
	rm -rf /var/lib/apt/lists/*; \
	locale-gen en_US.UTF-8

ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

ENV JAVA_HOME=/usr/lib/jvm/java-1.11-openjdk/jre
RUN mkdir -p $JAVA_HOME
COPY --from=builder /jre $JAVA_HOME/
RUN ln -svT $JAVA_HOME/bin/java /usr/local/bin/java

RUN mkdir -p /stream_perf_test
WORKDIR /stream_perf_test
COPY --from=builder /stream_perf_test ./
RUN java -jar stream-perf-test.jar --help

ENTRYPOINT ["java", "-Dio.netty.processId=1", "-jar", "stream-perf-test.jar"]
