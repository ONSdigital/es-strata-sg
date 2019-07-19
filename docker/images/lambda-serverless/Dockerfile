FROM node:10.16.0
USER root
RUN npm install -g serverless@1.44.1
RUN set -eux; \
  apt-get update; \
  apt-get install -y gosu; \
  rm -rf /var/lib/apt/lists/*;
COPY ./docker/files/entrypoint.sh /usr/local/bin/entrypoint.sh 
RUN chmod +x /usr/local/bin/entrypoint.sh 
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
