# trino-funnel-udf
funnel udf for trino

# usage
put files into trino and execute follow command     
```
mvn clean package -DskipTests -rf :trino-kdc-functions
```

# functions

## nvl
```
select nvl(column, 'test') from table;
```

## funnel
```
select ouid, funnel(event, timestamp, arrayp['step1'], array['step2'], array['step3']) from table group by ouid;
```

## array_sum
```
select array_sum(funnel) from (expresion) ;
```
