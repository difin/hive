FROM hive:${hive.tag}
ARG IMPALA_VERSION
USER root
RUN mkdir -p /usr/lib/impala/
# we use add because it extracts the tar gz (and thus saves image size)
ADD impala-fesupport-${IMPALA_VERSION}.tar.gz /usr/lib/impala/
RUN ln -s /usr/lib/impala/impala-fesupport-${IMPALA_VERSION}/*.so* /usr/lib/hadoop/lib/native
# install openssl compatibility library due to DWX-12786. This is due to impala being built on centos7 and this image
# being based on ubi8.
RUN yum -y update && \
  yum install -y compat-openssl10 && \
  yum clean all && \
  rm -rf /var/cache/yum

# add notice file
ADD /NOTICE.txt /NOTICE.txt

USER hive
ENV LD_LIBRARY_PATH /lib/x86_64-linux-gnu:/usr/lib/hadoop/lib/native:/usr/lib/impala/impala-fesupport-${IMPALA_VERSION}
