## Third homework SQL
Calculate monthly and weekly average growth_rate

Before usage have to run `./setup_db` which locate in `admin` directory 


arguments:  

Options   | Description
:-------- | :-------
-o --open | set open calculation_mode
-c --close| set close calculation_mode
-m --month| set month time_mode
-w --week | set week time_mode

*Default arguments calculation_mode="open", time_mode="month"*

#### Start
To run docker compose write in console  
`./get_growth_up.sh`

#### Examples
`./get_growth_up.sh -o -m`  
`./get_growth_up.sh -o -w`
`./get_growth_up.sh -c -m`
`./get_growth_up.sh -c -w`



