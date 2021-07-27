## Seventh homework CI_CD
Calculate monthly growth_rate

1. build images with jenkins
2. push images to docker hub
3. deploy images in aws EC2


`docker compose up -d` 

1. Before usage have to compiler project with maven `mvn clean install`. 
2. After that `cd target`.
3. You have to check your `Xmx` java variable value and if less than 2500mb add to each program run 
    `-Xmx2500mb` parameter, for example `java -Xmx2500mb 6_Java-1.0-SNAPSHOT.jar`. 
   This command help you to check your Xmx variable value `java -XX:+PrintFlagsFinal -version | grep -iE 'HeapSize|PermSize|ThreadStackSize'`
4. And run `java -jar 6_Java-1.0-SNAPSHOT.jar -sdb`.  Flag `-sdb`  indicates that it will create databases with empty tables.

arguments:  

Options                | Description
:----------------------|:---------------------------------------------------
--calculation_mode -cm | can be open-close as "oc" or close-close as "cc"
--load_mode -lm        | set load mode, can be "incr" or full, when we choose "full" will be truncated all audit and stg tables
--setup_db -sdb        | this option hasn't arguments and indicates that it will create databases with empty tables
--get_result -gr       | this option hasn't arguments and indicates that it will take result from destination table without any calculating


*Default arguments calculation_mode="oc", load_mode = "incr"*

#### Run Examples
`java -jar 6_Java-1.0-SNAPSHOT.jar`   
`java -jar 6_Java-1.0-SNAPSHOT.jar -cm oc`  
`java -jar 6_Java-1.0-SNAPSHOT.jar -cm cc -lm full`  
`java -jar 6_Java-1.0-SNAPSHOT.jar -gr`  

result:  
![results](screenshots/java_results.jpg)

