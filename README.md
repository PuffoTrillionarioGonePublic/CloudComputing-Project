## run hadoop

### generate bloom filters
```sh
cd hadoop/hadoop-optimized
mvn package
time hadoop jar target/cloud-project-1.0-SNAPSHOT.jar /inputs/title.basics.tsv /inputs/title.ratings.tsv /out/merge /out/count /out/bf 0.1 15
```

### test bloom filters
```sh
cd hadoop/test-hadoop
mvn package
time hadoop jar target/test-hadoop-1.0-SNAPSHOT.jar /out/bf /out/merge /out/test-results
```


## run spark

### generate bloom filters
```sh
cd spark
zip -r pyfiles.zip util/
time spark-submit --py-files pyfiles.zip --num-executors 4 --executor-cores 2 main.py /inputs/ out/ 0.01 --no-test 2>/dev/null
```

### test bloom filters
```sh
cd spark
zip -r pyfiles.zip util/
time spark-submit --py-files pyfiles.zip --num-executors 4 --executor-cores 2 main.py /inputs/ out/ 0.01 --no-calculate 2>/dev/null
```
