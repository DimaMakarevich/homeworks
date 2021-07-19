#!/usr/bin/env bash

path_to_usp=C:/Users/DmitryMakarevich/Desktop/homeworks/3_SQL/admin/usp

parse_arguments() {
 default_mode="open"
 default_time="month"
 for arg in "$@"; do
    case "$arg" in
      "-o" | "--open" )
           ;;
      "-c" | "--close" )
           default_mode="close"
           ;;
      "-m" | "--month" )
           ;;
      "-w" | "--week" )
          default_time="week"
           ;;
      *)
        echo "Wrong arguments" "${arg}"
        exit 1
    esac
 done

}


run_usp() {
  if [[ "${default_mode}" == "open" ]] ; then
    if [[ "${default_time}" == "month" ]] ; then
      mysql --user=root --password=Dima1790426 --host=localhost --port=3306 --database=forex_db < "${path_to_usp}`
      `/usp_get_month_growth_up_open_close.sql"
    else
       mysql --user=root --password=Dima1790426 --host=localhost --port=3306 --database=forex_db < "${path_to_usp}`
       `/usp_get_week_growth_up_open_close.sql"
    fi
  else
    if [[ "${default_time}" == "month" ]] ; then
      mysql --user=root --password=Dima1790426 --host=localhost --port=3306 --database=forex_db < "${path_to_usp}`
      `/usp_get_month_growth_up_close_close.sql"
    else
      mysql --user=root --password=Dima1790426 --host=localhost --port=3306 --database=forex_db < "${path_to_usp}`
      `/usp_get_week_growth_up_close_close.sql"
    fi
 fi

}



parse_arguments "$@"

run_usp