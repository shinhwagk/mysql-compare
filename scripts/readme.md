```sh
docker build --platform linux/amd64 -t shinhwagk/tmp_dbcompare:9 .

docker push shinhwagk/tmp_dbcompare:9
```



### dbrepair example
```sh
export ARGS_SOURCE_DSN=ghost/54448hotINBOX@172.16.0.179:3306
export ARGS_TARGET_DSN="dtle_sync/Ej4VFMyQ7wjRjtfX@172.16.0.116:3306"
export ARGS_LOG_LOCATION=/opt


export ARGS_SOURCE_DSN="devro/rYfulPFLeQ1v9Zb@10.50.10.190:3307"
export ARGS_TARGET_DSN='dtle_sync/Ej4VFMyQ7wjRjt!X@10.50.0.198:3317'
export ARGS_LOG_LOCATION=/opt
# export ARGS_REPAIR=true

docker run -it --rm -v `pwd`:/opt/ shinhwagk/tmp_dbcompare:4.2 bash
```