## Fourth homework HIVE
Calculate monthly and weekly average growth_rate

1. Before usage have to up hive cluster, go to docker-hive directory and do main steps. 
2. After that `docker cp 4_Hive  docker-hive_hive-server_1:/homeworks/`, copy scripts and data to docker.  
3. In docker-container in  directory `admin/setup_sh_scripts run` `./download_tools`.
4. Run `./setup_db` which locate in `admin/setup_sh_scripts run` directory.


arguments:  

Options   | Description
:-------- | :-------
-oc       | set open calculation_mode
-cc       | set close calculation_mode


*Default arguments calculation_mode="open"*

#### Start

`hplsql -e "INCLUDE '/homeworks/4_Hive/admin/sql/usp/usp_etl_stg2_to_destination.sql' CALL usp_etl_stg2_to_destination();"`

#### Examples
`hplsql -e "INCLUDE '/homeworks/4_Hive/admin/sql/usp/usp_etl_stg2_to_destination.sql' CALL usp_etl_stg2_to_destination('oc'');"`  
`hplsql -e "INCLUDE '/homeworks/4_Hive/admin/sql/usp/usp_etl_stg2_to_destination.sql' CALL usp_etl_stg2_to_destination('cc'');"`  



