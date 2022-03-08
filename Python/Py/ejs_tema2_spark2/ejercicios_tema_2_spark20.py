# -*- coding: utf-8 -*-
"""Del ejercicio de M&M aplicar: 


1.   Otras operaciones de agregación como el Max con otro tipo de 
ordenamiento (descendiente).
2.   Hacer un ejercicio como el “where” de CA que aparece en el libro pero 
indicando más opciones de estados (p.e. NV, TX, CA, CO).
3. Hacer un ejercicio donde se calculen en una misma operación el Max, 
Min, Avg, Count. Revisar el API (documentación) donde encontrarán 
este ejemplo:
ds.agg(max($"age"), avg($"salary"))
ds.groupBy().agg(max($"age"), avg($"salary"))
NOTA: $ es un alias de col()
4. Hacer también ejercicios en SQL creando tmpView
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import max, desc, asc, avg, count
import os

if __name__ == "__main__":

    mnm_path = os.path.realpath(os.path.join(os.path.dirname(__file__), '../../..', 'data', 'mnm_dataset.csv'))

    # Create SparkSession
    spark = (SparkSession
             .builder
             .appName("PythonMnMCount")
             .getOrCreate())
    # Read dataset
    mnm_df = (spark.read.format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .load(mnm_path))


    def mnm_op():
        # Select columns State, Color and Count, grouped by State and Color, aggregate Count with average, alias "Avg", order by "Avg"
        op_mnm_df = (mnm_df
                     .select("State", "Color", "Count")
                     .groupBy("State", "Color")
                     .agg(avg("Count").alias("Avg"))
                     .orderBy("Avg", ascending=True))
        # Show 60 rows without any truncate and print the total of rows
        op_mnm_df.show(n=60, truncate=False)
        print("Total Rows = %d" % (op_mnm_df.count()))
        # Same as before but only from California
        ca_op_mnm_df = (mnm_df
                        .select("State", "Color", "Count")
                        .where(mnm_df.State == "CA")
                        .groupBy("State", "Color")
                        .agg(avg("Count").alias("Avg"))
                        .orderBy(desc("Avg")))
        # Show only 10 rows without truncate
        ca_op_mnm_df.show(n=10, truncate=False)


    mnm_op()


    def mnm_multiple_states():
        # Select columns State, Color and Count where state in [CA, WA, NV, TX], grouped by State and Color, aggregate Count with count, alias "Total", order by "Total"
        multi_sats_op_mnm_df = (mnm_df
                                .where(
            (mnm_df.State == "CA") | (mnm_df.State == "WA") | (mnm_df.State == "NV") | (mnm_df.State == "TX"))
                                .groupBy("Color", "State")
                                .agg(count("Count").alias("Total"))
                                .orderBy(desc("Total"))
                                .select("State", "Color", "Total"))
        # Show 60 rows without truncate
        multi_sats_op_mnm_df.show(n=60, truncate=False)


    mnm_multiple_states()


    def mnm_multiple_agg():
        # Select columns State, Color, Total, Max, Avg, grouped by State and Color, aggregate Count with (count alias "Total", max alias "Max", avg alias "Avg"), order by "State"
        multi_agg_op_mnm_df = (mnm_df
                               .groupBy("State", "Color")
                               .agg(count("Count").alias("Total"), max("Count").alias("Max"), avg("Count").alias("Avg"))
                               .orderBy(desc("State"))
                               .select("State", "Color", "Total", "Max", "Avg"))
        # Show 60 without truncate
        multi_agg_op_mnm_df.show(n=60, truncate=False)


    mnm_multiple_agg()


    def mnm_pysql():
        mnm_df.createOrReplaceTempView("mnm_view")
        # Select columns State, Color and Count where State in [CA, TX, WA], grouped by State and Color, aggregate Count with average, order by "State"
        sql_mnm_view = spark.sql("select State, Color, avg(Count) from mnm_view \
            where State = 'CA' or State = 'TX' or State = 'WA' group by State, Color order by State;")
        # Show 60 without truncate
        sql_mnm_view.show(n=60, truncate=False)


    mnm_pysql()

    # Stop the SparkSession
    spark.stop()
