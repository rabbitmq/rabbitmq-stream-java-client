FROM ubuntu:20.04 as builder

ARG stream_perf_test_url="set-url-here"

RUN set -eux; \
	\
	apt-get update; \
	apt-get install --yes --no-install-recommends \
		ca-certificates \
		wget \
		gnupg \
		jq

ARG JAVA_VERSION="17"

RUN if [ "$(uname -m)" = "aarch64" ] || [ "$(uname -m)" = "arm64" ]; then echo "ARM"; ARCH="arm"; BUNDLE="jdk"; else echo "x86"; ARCH="x86"; BUNDLE="jdk"; fi \
    && wget "https://api.azul.com/zulu/download/community/v1.0/bundles/latest/?java_version=$JAVA_VERSION&ext=tar.gz&os=linux&arch=$ARCH&hw_bitness=64&release_status=ga&bundle_type=$BUNDLE" -O jdk-info.json
RUN wget --progress=bar:force:noscroll -O "jdk.tar.gz" $(cat jdk-info.json | jq --raw-output .url)
RUN echo "$(cat jdk-info.json | jq --raw-output .sha256_hash) *jdk.tar.gz" | sha256sum --check --strict -

RUN set -eux; \
    if [ "$(uname -m)" = "x86_64" ] ; then JAVA_PATH="/usr/lib/jdk-$JAVA_VERSION"; \
    mkdir $JAVA_PATH && \
    tar --extract  --file jdk.tar.gz --directory "$JAVA_PATH" --strip-components 1; \
	  $JAVA_PATH/bin/jlink --compress=2 --output /jre --add-modules java.base,jdk.management,java.naming,java.xml,jdk.unsupported,jdk.crypto.cryptoki,jdk.httpserver; \
	  /jre/bin/java -version; \
	  fi

RUN set -eux; \
    if [ "$(uname -m)" = "aarch64" ] || [ "$(uname -m)" = "arm64" ] ; then JAVA_PATH="/jre"; \
    mkdir $JAVA_PATH && \
    tar --extract  --file jdk.tar.gz --directory "$JAVA_PATH" --strip-components 1; \
	  fi

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
    wget --progress dot:giga --output-document "/usr/local/src/stream-perf-test.jar.asc" "$stream_perf_test_url.asc"; \
    wget --progress dot:giga --output-document "/usr/local/src/stream-perf-test.jar" "$stream_perf_test_url"; \
    STREAM_PERF_TEST_SHA256="$(wget -qO- $stream_perf_test_url.sha256)"; \
    echo "$STREAM_PERF_TEST_SHA256 /usr/local/src/stream-perf-test.jar" | sha256sum --check --strict -; \
    \
    export GNUPGHOME="$(mktemp -d)"; \
    gpg --batch --keyserver "$PGP_KEYSERVER" --recv-keys "$RABBITMQ_PGP_KEY_ID"; \
    gpg --batch --verify "/usr/local/src/stream-perf-test.jar.asc" "/usr/local/src/stream-perf-test.jar"; \
    gpgconf --kill all; \
    rm -rf "$GNUPGHOME"; \
    \
    mkdir -p "$STREAM_PERF_TEST_HOME"; \
    cp /usr/local/src/stream-perf-test.jar $STREAM_PERF_TEST_HOME/stream-perf-test.jar

FROM ubuntu:20.04

# we need locales support for characters like µ to show up correctly in the console
RUN set -eux; \
	apt-get update; \
	apt-get install -y --no-install-recommends \
		locales \
		wget \
	; \
	rm -rf /var/lib/apt/lists/*; \
	locale-gen en_US.UTF-8

ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk/jre
RUN mkdir -p $JAVA_HOME
COPY --from=builder /jre $JAVA_HOME/
RUN ln -svT $JAVA_HOME/bin/java /usr/local/bin/java

RUN mkdir -p /stream_perf_test
WORKDIR /stream_perf_test
COPY --from=builder /stream_perf_test ./
RUN set -eux; \
    if [ "$(uname -m)" = "x86_64" ] ; then java -jar stream-perf-test.jar --help ; \
	  fi

RUN groupadd --gid 1000 stream-perf-test
RUN useradd --uid 1000 --gid stream-perf-test --comment "perf-test user" stream-perf-test

USER stream-perf-test:stream-perf-test

ENTRYPOINT ["java", "-Dio.netty.processId=1", "-jar", "stream-perf-test.jar"]
