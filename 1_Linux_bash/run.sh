#!/usr/bin/env bash

run_service(){
  local os="${1}"
  if [[ "${os}" == "centos" ]] ; then
    docker compose up -d mysql_db mongo_db centos
  elif [[ "${os}" == "ubuntu" ]] ; then
    docker compose up -d mysql_db mongo_db ubuntu
  else
    docker compose up -d
  fi
}

parse_arguments() {
 local arg
 if [[ $# -gt 0 ]] ; then
   arg="${1}"
   shift
   case "${arg}" in
    -c | --centos)
      run_service "centos"
      ;;
    -u | --ubuntu)
      run_service "ubuntu"
      ;;
    -a | --all)
      run_service "all"
      ;;
    *)
      prinf "Wrong argument: %s\n" "${arg}"
      exit 1
   esac
 else
   run_service "all"
 fi
}

parse_arguments "${@}"