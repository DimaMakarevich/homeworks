#!/usr/bin/env bash

get_credentials(){
  local readonly file = "1_Linux_bash/user.txt"
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

create_mongodb_user() {
mongo --host host.docker.internal --27017 << EOF
  use admin
  if (db.system.users.findOne({user: "${DOCKER_USER}"}) == null) {
    db.createUser({
      user: "${DOCKER_USER}",
      pwd: "${DOCKER_PASSWORD}",
      roles: ["userAdminAnyDataBase", "dbAdminAneDatabase", "readWriteAnyDataBase"],
      mechanisms: ["SCRAM-SHA-1"]})
  }
EOF
}

create_mysql_user() {
  local readonly CREATE_USER="CREATE USER IF NOT EXISTS '${DOCKER_USER}'@'localhost' IDENTIFIED BY '${DOCKER_PASSWORD}';"
  local readonly GRANT_PRIVILEGES="GRANT ALL PRIVILEGES ON *.* TO '${DOCKER_USER}'@'localhost';"
  local readonly FLUSH_PRIVILEGES="FLUSH PRIVILEGES;"
  local readonly SQL="${CREARE_USER}${GRANT_PRIVILEGES}${FLUSH_PRIVILEGES}"

  mysql -u root -h host.docker.internal -P 3310 -e "${SQL}"
}

create_mongodb_user
create_mysql_user