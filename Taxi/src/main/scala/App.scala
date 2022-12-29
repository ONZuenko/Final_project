import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, FloatType, TimestampType, BooleanType, StructType}
import org.apache.spark.sql.functions._

object RDDParallelize {

  def main(args: Array[String]): Unit={
    val spark:SparkSession=SparkSession.builder().master(master="local[1]")
      .appName(name="SparkByExample.com")
      .getOrCreate()
    //import spark.implicits._

    // Создание схемы
    val schema = new StructType()
      .add(name="VendorId", IntegerType)
      .add("Trep_pickup_datetime", TimestampType)
      .add("Trep_dropoff_datetime", TimestampType)
      .add("Passanger_count", IntegerType)
      .add("Trip_distance", FloatType)
      .add("Ratecodeid", IntegerType)
      .add("Store_and_fwd_flag", StringType)
      .add("PulocationId", IntegerType)
      .add("Dolocationid", IntegerType)
      .add("Payment_type", BooleanType)
      .add("Fare_amount", FloatType)
      .add("extra", FloatType)
      .add("Mta_tax", FloatType)
      .add("Tip_amount", FloatType)
      .add("Tools_amount", FloatType)
      .add("Improvement_surchange", FloatType)
      .add("Total_amount", FloatType)
      .add("Congestion_surchange", FloatType)

    // Заполнение датафрейма из файла
    val Initial_df = spark
      .read
      .format("csv")
      .schema(schema)
      .load("yellow_tripdata_2020-01.csv")

    // Выбор только нужных столбцов, удаление пустых строк
    var Work_df =  Initial_df.select("Trep_pickup_datetime", "Passanger_count", "Fare_amount").na.drop()

    //Преобразование формата даты
    Work_df=Work_df.withColumn("Trep_pickup_datetime", to_date(column("Trep_pickup_datetime"), "yyyy-mm-dd"))

    //Подсчет процентов поездок по количеству человек в машине,
    //а также самой дорогой и самой дешевой поездки для каждой группы
    Work_df.createOrReplaceTempView("YelowTaxi")
    Work_df=spark.sql(" SELECT Trep_pickup_datetime, "+
      "ROUND(COUNT(case when Passanger_count==0 then 1 else NULL end)/COUNT(Passanger_count)*100, 2) percentage_zero, "+
      "MAX(case when Passanger_count==0 then Fare_amount else 0 end) exp_zero, "+
      "MIN(case when Passanger_count==0 then Fare_amount else 0 end) cheap_zero, "+
      "ROUND(COUNT(case when Passanger_count==1 then 1 else NULL end)/COUNT(Passanger_count)*100, 2) percentage_1p, "+
      "MAX(case when Passanger_count==1 then Fare_amount else 0 end) exp_1p, " +
      "MIN(case when Passanger_count==1 then Fare_amount else 0 end) cheap_1p, " +
      "ROUND(COUNT(case when Passanger_count==2 then 1 else NULL end)/COUNT(Passanger_count)*100, 2) percentage_2p, "+
      "MAX(case when Passanger_count==2 then Fare_amount else 0 end) exp_2p, " +
      "MIN(case when Passanger_count==2 then Fare_amount else 0 end) cheap_2p, " +
      "ROUND(COUNT(case when Passanger_count==3 then 1 else NULL end)/COUNT(Passanger_count)*100, 2) percentage_3p, "+
      "MAX(case when Passanger_count==3 then Fare_amount else 0 end) exp_3p, " +
      "MIN(case when Passanger_count==3 then Fare_amount else 0 end) cheap_3p, " +
      "ROUND(COUNT(case when Passanger_count>=4 then 1 else NULL end)/COUNT(Passanger_count)*100, 2) percentage_4p_plus, "+
      "MAX(case when Passanger_count>=4 then Fare_amount else 0 end) exp_4p_plus, " +
      "MIN(case when Passanger_count>=4 then Fare_amount else 0 end) cheap_4p_plus " +
      "FROM YelowTaxi GROUP BY Trep_pickup_datetime ORDER BY Trep_pickup_datetime")//.show()

    //Сохранение данных в parquet формат
    Work_df.coalesce(1).write.mode("overwrite").format("parquet").mode("append").save("taxi_result")

    //Формирование таблицы для построения графика и выгрузка данных
    var Ch_pass_tip=Initial_df.select("Passanger_count", "Tip_amount").na.drop().dropDuplicates()//.show()
    Ch_pass_tip=Ch_pass_tip.withColumn("Tip_amount", round(column("Tip_amount"))).dropDuplicates()
    Ch_pass_tip.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").mode("append").save("passenger")
    var Ch_dist_tip=Initial_df.select("Trip_distance", "Tip_amount").na.drop().dropDuplicates()
    Ch_dist_tip=Ch_dist_tip.withColumn("Trip_distance", round(column("Trip_distance")))
    Ch_dist_tip=Ch_dist_tip.withColumn("Tip_amount", round(column("Tip_amount"))).dropDuplicates()
    Ch_dist_tip.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").mode("append").save("distance")
  }
}
