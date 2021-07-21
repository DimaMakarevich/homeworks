#!/usr/bin/env bash

get_credentials(){
  local readonly file = "7_CI_CD/user.txt"
  DOCKER_USER="root"
  DOCKER_PASSWORD="root"
  while IFS='=' read -r name credential
    do
      if [["@{name}" = "user"]]; then
        DOCKER_USER="${credential}"
      elif [["@{name}" = "password"]]; then
        DOCKER_PASSWORD="${credential}"
      fi
    done < "${file}"
}
get_credentials

create_mysql_user() {
  local readonly CREATE_USER="CREATE USER IF NOT EXISTS '${DOCKER_USER}'@'localhost' IDENTIFIED BY '${DOCKER_PASSWORD}';"
  local readonly GRANT_PRIVILEGES="GRANT ALL PRIVILEGES ON *.* TO '${DOCKER_USER}'@'localhost';"
  local readonly FLUSH_PRIVILEGES="FLUSH PRIVILEGES;"
  local readonly SQL="${CREATE_USER}${GRANT_PRIVILEGES}${FLUSH_PRIVILEGES}"
  mysql -u root -h host.docker.internal -P 3310 -e "${SQL}"
}

create_mysql_user