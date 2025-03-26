FROM hive:${hive.tag}
ARG IMPALA_VERSION
USER root
RUN mkdir -p /usr/lib/impala/
# we use add because it extracts the tar gz (and thus saves image size)
ADD impala-fesupport-${IMPALA_VERSION}.tar.gz /usr/lib/impala/
RUN ln -s /usr/lib/impala/impala-fesupport-${IMPALA_VERSION}/*.so* /usr/lib/hadoop/lib/native

# add notice file
ADD /NOTICE.txt /NOTICE.txt

USER hive
ENV LD_LIBRARY_PATH /lib/x86_64-linux-gnu:/usr/lib/hadoop/lib/native:/usr/lib/impala/impala-fesupport-${IMPALA_VERSION}
