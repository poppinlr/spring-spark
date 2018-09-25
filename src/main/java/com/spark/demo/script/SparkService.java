package com.spark.demo.script;

import lombok.extern.log4j.Log4j2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;

import static org.apache.spark.sql.functions.*;

@Service
@Log4j2
public class SparkService {

    private static final String cdpath = "/root/charge_details.parquet";
    private static final String rspath = "/root/register_settlement.parquet";

    @Autowired
    private SQLContext sqlContext;

    public void test() {
        Dataset<Row> dataset = sqlContext.read().parquet(rspath);
        Long day = 86400 * 30L;
        WindowSpec w = Window.partitionBy(col("PER_NO")).orderBy(unix_timestamp(col("IN_HOSP_DATE"))).rangeBetween(0, day);
        //.colRegex()//.alias("topCts")

        dataset.as(Encoders.bean(RegisterSettlement.class))
                .filter(rs -> rs.getIN_HOSP_DATE().after(new Timestamp(0)))
                .withColumn("cts", count(col("PER_NO")).over(w)).orderBy(desc("cts"), desc("PER_NO"))
                .where("cts >10")
                .groupBy(col("PER_NO"))
                .agg(max("cts").alias("max_count"))
                .orderBy(desc("max_count"))
                .show();
//        dataset.as(Encoders.bean(RegisterSettlement.class))
//                .filter(rs -> rs.getIN_HOSP_DATE().after(new Timestamp(0)))
//                .where("PER_NO = 02202115802")
//                .withColumn("cts", row_number().over(w)).orderBy(desc("cts"))
//                .where("cts >10")
//                .select("PER_NO", "cts", "IN_HOSP_DATE")
//                .show(1000);

//        dataset.as(Encoders.bean(RegisterSettlement.class))
//                .withColumn("cast", col("IN_HOSP_DATE").cast("timestamp").cast("long"))
//                .withColumn("unix_timestamp", unix_timestamp(col("IN_HOSP_DATE")))
//                .select("cast", "unix_timestamp")
//                .show();

//        dataset.as(Encoders.bean(RegisterSettlement.class))
//                .where("IN_HOSP_DATE > '2018-01-19' and IN_HOSP_DATE < '2018-02-21' and PER_NO = 02202115802")
//                .select("PER_NO", "IN_HOSP_DATE")
//                .show(200);


//        RegisterSettlement rs = new RegisterSettlement();
//        rs.setPER_NO("1");
//        rs.setIN_HOSP_DATE(0);
//
//        RegisterSettlement rs1 = new RegisterSettlement();
//        rs1.setPER_NO("2");
//        rs1.setIN_HOSP_DATE(5);
//
//        RegisterSettlement rs2 = new RegisterSettlement();
//        rs2.setPER_NO("2");
//        rs2.setIN_HOSP_DATE(10);
//
//        List<RegisterSettlement> list = Lists.newArrayList(rs, rs1, rs2);


//
//
//        Dataset<RegisterSettlement> dataset = sqlContext.createDataset(list, Encoders.bean(RegisterSettlement.class));
//        WindowSpec w = Window.orderBy(col("IN_HOSP_DATE")).rangeBetween(-6, 6);
//        WindowSpec w = Window.orderBy(unix_timestamp(col("IN_HOSP_DATE"))).rangeBetween(-6, 6);
//        dataset
//                .withColumn("cts", count(col("PER_NO")).over(w)).orderBy(asc("IN_HOSP_DATE"))
//                .where("cts > 2")
//                .show();
//        log.info("-------------------------");
//        dataset
//                .withColumn("cts", count(col("PER_NO")).over(w)).orderBy(asc("IN_HOSP_DATE"))
//                .show();
//
//
//
//
//        Dataset<Row> dataset = sqlContext.read()
//                .parquet(rspath);

//        dataset.
//        log.info("Arrays.stream(dataset.schema().fieldNames()).forEach(log::info) start--- ");
//        Arrays.stream(dataset.schema().fields()).forEach(f -> {
//            log.info(f.name());
//            log.info(f.dataType().simpleString());
//        });
//        log.info("Arrays.stream(dataset.schema().fieldNames()).forEach(log::info) end----- ");
//        Dataset<RegisterSettlement> set =
//        dataset.as(Encoders.bean(RegisterSettlement.class))
//                .filter(rs -> "1".equals(rs.getVISIT_TYPE()));
//
//
////        dataset.as(Encoders.bean(RegisterSettlement.class))
////        .groupBy("");
////        sqlContext.
////        Dataset<RegisterSettlement> rs = dataset.as(Encoders.bean(RegisterSettlement.class));
//        Long day = 60*60*1000L * 24;
//
//        WindowSpec w = Window.partitionBy(col("date")).rangeBetween(0, 3 * day);
//        rs.withColumn("cts", count(col("PER_NO")).over(w)).orderBy(asc("IN_HOSP_DATE"))
//        .where("cts > 10")
//        .show();
//        set.withColumn("date", datediff(col("IN_HOSP_DATE"), "MMMM-YY-DD"))
//                .withColumn("cts", count(col("PER_NO")).over(w))
//                .show();
//        set.withColumn("cts", count(col("PER_NO")).over(w))
//                .show();
//        dataset.as(Encoders.bean(RegisterSettlement.class))
//                .withColumn("cts", count(col("PER_NO")).over(w)).orderBy(asc("IN_HOSP_DATE"))
//                .show();

//        JavaPairRDD<TimeStamp, RegisterSettlement> javaPairRDD = dataset.as(Encoders.bean(RegisterSettlement.class))
//                .javaRDD()
//                .mapToPair(rdd -> new Tuple2(rdd.getIN_HOSP_DATE(), rdd));
//        JavaPairDStream<Timestamp, RegisterSettlement> jpds = dataset.as(Encoders.bean(RegisterSettlement.class))
//                .toJavaRDD()
//                .;
//
//
//
////        JavaDStream<RegisterSettlement> jds = dataset.
//
//        List<RegisterSettlement> rsList = dataset.as(Encoders.bean(RegisterSettlement.class))
//                .collectAsList()
//                .parallelStream()
//                .sorted(Comparator.comparing(RegisterSettlement::getIN_HOSP_DATE))
//                .collect(Collectors.toList());
//
//        rsList.parallelStream().filter(rs -> !StringUtils.isEmpty(rs.getIN_HOSP_DATE()))
//                .findAny()
//                .ifPresent( d -> log.info(d.getIN_HOSP_DATE()));
////        rsList.forEach(rs -> {
////            LocalDate fromDate = rs.getIN_HOSP_DATE();
////            Optional<RegisterSettlement> toDate = rsList.parallelStream().filter(r -> r.getIN_HOSP_DATE().equals(fromDate.plusDays(30))).findAny();
////        });
//
//        List<List<RegisterSettlement>> list =
//                IntStream.range(0, rsList.size())
//                        .filter(i -> "1".equals(rsList.get(i).getVISIT_TYPE()))
//                        .mapToObj(i -> {
//                            LocalDate fromDate = rsList.get(i).getIN_HOSP_DATE().toLocalDateTime().toLocalDate();
//                            OptionalInt toDate = IntStream.range(i, rsList.size())
//                                    .filter(j -> rsList.get(j).getIN_HOSP_DATE().toLocalDateTime().toLocalDate().equals(fromDate.plusDays(31)))
//                                    .findFirst();
//                            if (toDate.isPresent()) {
//                                List<RegisterSettlement> l = rsList.subList(i, toDate.getAsInt());
//                                return l.parallelStream()
//                                        .collect(Collectors.groupingBy(RegisterSettlement::getPER_NO))
//                                        .entrySet()
//                                        .parallelStream()
//                                        .filter(entry -> entry.getValue().size() > 10)
//                                        .map(Map.Entry::getValue)
//                                        .collect(Collectors.toList());
//                            } else {
//                                return null;
//                            }
//                        })
//                        .filter(Objects::nonNull)
//                        .flatMap(List::stream)
//                        .collect(Collectors.toList());
//
//        list.forEach(l -> log.info("per no : " + l.get(0).getPER_NO() + ", 就诊次数: " + l.size()));


//        Map<String, List<RegisterSettlement>> map = dataset
//                .as(Encoders.bean(RegisterSettlement.class))
//                .collectAsList()
//                .parallelStream()
//                .filter(detail -> "1".equals(detail.getVISIT_TYPE()))
//                .collect(Collectors.groupingBy(RegisterSettlement::getPER_NO));
////
//        log.info("---- Collectors.groupingBy(ChargeDetail::getPerNo) done---");



//
//        map.entrySet()
//                .parallelStream()
//                .forEach(set -> {
//                    StreamEx.ofSubLists(set.getValue(), 30)
//                            .filterBy()
//                    if (set.getValue().size() > 10) {
//                        log.error("perNo: " + set.getKey());
//                    }
//                });

//        Optional<ChargeDetail> chargeDetail = chargeDetailDataset.collectAsList()
//                .stream()
//                .findFirst();
//
//        if (chargeDetail.isPresent()) {
//            ChargeDetail detail = chargeDetail.get();
//            log.info("--- ---" + detail.getRegisterNo());
//
//        }
//
//
//
//        Dataset<ChargeDetail> set = dataset
//                .map(row -> {
//                    return new ChargeDetail();
//                    }, Encoders.bean(ChargeDetail.class));
//
//        set.collectAsList()
//                .stream()
//                .limit(1)
//                .findAny();
    }
}
