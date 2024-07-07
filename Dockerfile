FROM node:20-bullseye as webui-build

WORKDIR /usr/src/app/webui-build
COPY ./webui/package*.json ./
RUN npm i
COPY ./webui/. .
RUN npm run build

FROM azul/zulu-openjdk:21 as server-build

WORKDIR /usr/src/app/server-build
COPY ./server/.mvn .mvn
COPY ./server/mvnw ./server/pom.xml ./
RUN ./mvnw quarkus:go-offline
# The previous thing still doesn't download 100% everything
RUN ./mvnw -Dmaven.test.skip=true -Dskip.unit=true package --fail-never
COPY ./server/. .
RUN ./mvnw -Dmaven.test.skip=true -Dskip.unit=true clean package

FROM azul/zulu-openjdk-alpine:21-jre-headless

RUN apk update && apk add fuse && rm -rf /var/cache/apk/*

WORKDIR /usr/src/app
COPY --from=server-build /usr/src/app/server-build/target/quarkus-app/. .
RUN mkdir -p webui
COPY --from=webui-build /usr/src/app/webui-build/dist/. ./webui

ENV dhfs_webui_root=/usr/src/app/webui

COPY ./dockerentry.sh .

RUN ["chmod", "+x", "./dockerentry.sh"]

CMD [ "./dockerentry.sh" ]