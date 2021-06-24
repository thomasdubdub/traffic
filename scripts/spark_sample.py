import findspark
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as fn

from traffic.core import Traffic
from traffic.data.datasets import landing_zurich_2019

# from pyspark.sql import types as typ


class TrafficSpark(Traffic):
    def eval(self, *args, **kwargs) -> Traffic:
        pass

    @property
    def traffic(self) -> Traffic:
        return self.eval()


def init_spark() -> SparkSession:

    findspark.init()

    conf = SparkConf()
    conf.setAppName("my_spark_app")
    conf.setMaster("spark://localhost:7077")
    conf.set("spark.executor.memory", "600G")
    conf.set("spark.driver.memory", "12G")
    conf.set("spark.driver.maxResultSize", "12G")
    conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    conf.set("spark.sql.session.timeZone", "UTC")

    return SparkSession.builder.config(conf=conf).getOrCreate()


def main_vanilla() -> None:
    t = (
        landing_zurich_2019.query("altitude < 10000")
        .resample("1s")
        .filter()
        .has("aligned_on_LSZH")
        .has("goaround")
        .eval(desc="", max_workers=8)
    )
    t.to_parquet("goaround_dataset.parquet")


def main_spark():
    session = init_spark()

    ts: TrafficSpark = landing_zurich_2019.spark(session)
    ts = TrafficSpark(landing_zurich_2019.data, session=session)

    ts2: TrafficSpark = (
        ts.filter(fn.col("altitude") < 10000)  # this should work naturally
        .resample("1s")
        .filter()  # the other semantic clashes... decide something
        .has("aligned_on_LSZH")
        .has("goaround")
    )

    t1 = ts2.eval()  # type: Traffic
    # t1 = ts2.traffic  # type: Traffic

    t1.to_parquet("goaround_dataset.parquet")
    # Can we apply the to_parquet directly on the TrafficSpark
