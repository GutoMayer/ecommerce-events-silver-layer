from pyspark.sql.types import *
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql import DataFrame
from datetime import date
from pyspark import StorageLevel
from dotenv import load_dotenv
import os

load_dotenv()


class SilverLayer:
    def __init__(self, data_referencia: str = None):
        hadoop_home = os.getenv("HADOOP_HOME")
        hadoop_bin  = os.getenv("HADOOP_BIN")

        if hadoop_home:
            os.environ["HADOOP_HOME"] = hadoop_home
        if hadoop_bin:
            os.environ["PATH"] += os.pathsep + hadoop_bin

        self.data_referencia = data_referencia or date.today().isoformat()

        self.spark = (
            SparkSession.builder
            .appName("silver_pipeline")
            .master("local[*]")
            .config("spark.driver.memory", "4g")
            .config("spark.executor.memory", "4g")
            .config("spark.executor.instances", "20")
            .config("spark.driver.maxResultSize", "4g")
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.files.openCostInBytes", str(4 * 1024 * 1024))
            .config("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))
            .getOrCreate()
        )
        

        self.schema_eventos = StructType([
            StructField("event_id",        StringType(),    nullable=False),
            StructField("event_timestamp", TimestampType(), nullable=False),
            StructField("event_type",      StringType(),    nullable=False),
            StructField("price_paid",      DoubleType(),    nullable=True),
            StructField("product_id",      StringType(),    nullable=False),
            StructField("quantity",        IntegerType(),   nullable=True),
            StructField("session_id",      StringType(),    nullable=False),
            StructField("user_id",         StringType(),    nullable=False),
            StructField("context", StructType([
                StructField("device",     StringType(), nullable=True),
                StructField("ip_hash",    StringType(), nullable=True),
                StructField("user_agent", StringType(), nullable=True),
            ]), nullable=True),
            StructField("marketing", StructType([
                StructField("utm_campaign", StringType(), nullable=True),
                StructField("utm_source",   StringType(), nullable=True),
            ]), nullable=True),
        ])

    def verifica_leitura_tabelas(self, df: DataFrame, name: str) -> bool:
        sample = df.take(5)
        if sample:
            print(f"{name} lida com sucesso.")
            for row in sample:
                print(row)
            return True
        print(f"Nenhum registro encontrado em {name}.")
        return False

    def escreve_parquet(
        self,
        df: DataFrame,
        path: str,
        mode: str = "overwrite",
        n_files: int = None,
        partition_col: str = None,
    ) -> None:
        if partition_col:
                        # repartition pela coluna garante exatamente 1 arquivo por valor de data
            (
                df
                .repartition(partition_col)
                .write
                .mode(mode)
                .partitionBy(partition_col)
                .option("maxRecordsPerFile", 1000000)
                .option("compression", "snappy")
                .parquet(path)
                
            )
        else:
            if n_files:
                df = df.repartition(n_files)
            df.write.mode(mode).option("compression", "snappy").parquet(path)

    def processa_eventos(self, path: str) -> DataFrame:
        """Lê os JSONs brutos, escreve em parquet consolidado e retorna o DataFrame."""
        eventos = (
            self.spark.read
            .schema(self.schema_eventos)
            .json(path)
            .select(
                "event_id",
                "event_timestamp",
                "event_type",
                "price_paid",
                "product_id",
                "quantity",
                "session_id",
                "user_id",
                F.col("context.device").alias("device"),
                F.col("context.ip_hash").alias("ip_hash"),
                F.col("context.user_agent").alias("user_agent"),
                F.col("marketing.utm_campaign").alias("utm_campaign"),
                F.col("marketing.utm_source").alias("utm_source"),
                F.to_date("event_timestamp").alias("dt"),
            )
        )
        if self.verifica_leitura_tabelas(eventos, "eventos"):
            # consolida os JSONs fragmentados em 30 arquivos parquet
            eventos_reduced = eventos.repartition(30)
            return eventos_reduced
        else:
            raise ValueError("Nenhum registro encontrado em eventos.")

    def ler_produtos(self, path_bruto: str, path_processado: str) -> DataFrame:
        """Lê produtos, consolida em 1 arquivo e retorna o DataFrame ."""
        produtos_raw = (
            self.spark.read.parquet(path_bruto)
            .select(
                F.col("product_id").cast(StringType()).alias("product_id"),
                "category",
                "subcategory",
                "brand",
                "price",
            )
        )
        
        df = produtos_raw.repartition(1)
        if self.verifica_leitura_tabelas(df, "produtos"):
            return df
        else:
            raise ValueError("Nenhum registro encontrado em produtos.")

    def ler_usuarios(self, path: str) -> DataFrame:
        """Lê usuários e retorna o DataFrame."""
        df = (
            self.spark.read.parquet(path)
            .select(
                F.col("user_id").cast(StringType()).alias("user_id"),
                "country",
                "age_band",
                "signup_date",
            )
        )
        if self.verifica_leitura_tabelas(df, "usuarios"):
            return df
        else:
            raise ValueError("Nenhum registro encontrado em usuários.")

    def join(
        self,
        eventos: DataFrame,
        produtos: DataFrame,
        usuarios: DataFrame,
    ) -> DataFrame:
        return (
            eventos.alias("e")
            .join(broadcast(produtos), "product_id", "left")
            .join(broadcast(usuarios), "user_id",    "left")
            .select(
                F.col("event_id"),
                F.col("event_timestamp"),
                F.col("event_type"),
                F.col("session_id"),
                F.col("user_id"),
                F.col("country"),
                F.col("age_band"),
                F.datediff(
                    F.lit(self.data_referencia).cast("date"),
                    F.col("signup_date")
                ).alias("dias_desde_signup"),
                F.col("product_id"),
                F.col("category"),
                F.col("subcategory"),
                F.col("brand"),
                F.col("price"),
                F.col("device"),
                F.col("utm_source"),
                F.col("utm_campaign"),
                F.col("quantity"),
                F.col("price_paid"),
                F.col("dt").alias("event_date"),
            )
        )

    def escreve_tabelas_silver(self, joined: DataFrame) -> None:
        """
        Escreve eventos válidos e rejeitados em parquet,
        particionados por event_date com 1 arquivo por data.
        Rejeitados = eventos sem match em produtos OU usuários.
        """
        valido = F.col("category").isNotNull() & F.col("country").isNotNull()

        try:
            self.escreve_parquet(
                df=joined.filter(valido),
                path="data/silver/eventos_enriquecidos/",
                partition_col="event_date",
            )
        except Exception as e:
            print(f"Erro ao escrever eventos enriquecidos: {e}")
            raise

        try:
            self.escreve_parquet(
                df=joined.filter(~valido),
                path="data/silver/eventos_rejeitados/"
            )
        except Exception as e:
            print(f"Erro ao escrever eventos rejeitados: {e}")
            raise

    def testes_minimos(self, df: DataFrame) -> None:
        metricas = df.agg(
            F.count("*").alias("total_registros"),
            F.sum(F.col("event_id").isNull().cast("int")).alias("event_id_nulos"),
            F.countDistinct("event_id").alias("event_id_distintos"),
            (F.count("*") - F.countDistinct("event_id")).alias("event_id_duplicados"),
        )
        metricas.show()


if __name__ == "__main__":
    s = SilverLayer(data_referencia="2026-04-20")

    path_eventos         = "dados_brutos/eventos/"
    path_prod_bruto      = "dados_brutos/produtos/"
    path_prod_processado = "data/silver/produtos/"
    path_users           = "dados_brutos/usuarios/"

    eventos_flat = s.processa_eventos(path_eventos).persist(StorageLevel.DISK_ONLY)

    produtos = s.ler_produtos(path_prod_bruto, path_prod_processado)
    usuarios = s.ler_usuarios(path_users)

    joined = s.join(eventos_flat, produtos, usuarios).persist(StorageLevel.DISK_ONLY)

    # libera inputs antes de usar joined para aliviar o heap
    eventos_flat.unpersist()

    s.escreve_tabelas_silver(joined)

    s.testes_minimos(joined)

    input("Pipeline finalizada. Pressione Enter para encerrar e liberar o Spark UI...")

    joined.unpersist()