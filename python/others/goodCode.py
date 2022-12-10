import json
from io import BytesIO

from pandas_profiling import ProfileReport
from pyspark.sql import functions
from pyspark.sql.types import LongType, StringType, StructField, StructType

from __app__.func_code.azure_storage import landing_container
from __app__.func_code.envs import DATA_QUALITY_CONTAINER
from __app__.func_code.logger import json_log, logger
from __app__.func_code.utils.spark_context import get_spark_session

ERROR_COLUMNS = ["rule", "row_index", "column", "value"]

NUMERIC_TYPES = ["int", "integer", "decimal", "long"]

fields = [
    StructField("rule", StringType(), True),
    StructField("row_index", LongType(), True),
    StructField("column", StringType(), True),
    StructField("value", StringType(), True),
]
schema = StructType(fields)


class DataQualityCheck:
    def __init__(
        self,
        source_container,
        source_path,
        contract,
        spark_session=None,
        includes_row_index=True,
        file_id=None,
    ):
        self._spark_session = spark_session or get_spark_session()
        self._source_container = source_container
        self._source_path = source_path
        self._contract = contract
        self.includes_row_index = includes_row_index
        self.file_id = file_id
        self._data = self._read_file()
        self.errors = self.spark_session.createDataFrame(
            self.spark_session.sparkContext.emptyRDD(), schema
        )
        self._summary = {}

    @property
    def spark_session(self):
        return self._spark_session

    @property
    def source_container(self):
        return self._source_container

    @property
    def source_path(self):
        return self._source_path

    @property
    def contract(self):
        return self._contract

    @property
    def schema(self):
        return self.contract.schema

    @property
    def file_options(self):
        return self.contract.prepare.file_options

    @property
    def file_type(self):
        return self.contract.prepare.file_type

    @property
    def data(self):
        return self._data

    @property
    def summary(self):
        return self._summary

    @property
    def output_container(self):
        return landing_container

    @property
    def output_file(self):
        return f"{DATA_QUALITY_CONTAINER}/{self.source_path}/data_quality.json"

    @property
    def output_profiling_file(self):
        return f"{DATA_QUALITY_CONTAINER}/{self.source_path}/data_profiling"

    @property
    def missing_columns(self):
        return [c for c in self.schema.file_fields if c not in self.data.schema.names]

    @property
    def condensed_summary(self):
        """
        Returns a condensed summary with high level details of the quality of the data.
        Basically, everything but the details of the errors.
        """
        condensed_summary = {k: v for k, v in self.summary.items() if k != "errors"}
        condensed_summary["summary_file"] = self.output_file
        condensed_summary["profiling_html"] = f"{self.output_profiling_file}.html"
        condensed_summary["profiling_json"] = f"{self.output_profiling_file}.json"
        return condensed_summary

    @property
    def error_unique_row_count(self):
        return self.errors.select("row_index").distinct().count()

    @property
    def quality_state(self):
        if self.error_unique_row_count > 0:
            return "failed"
        if self.data.count() == 0:
            return "failed"
        if self.missing_columns:
            return "failed"
        return "ok"

    def infer_schema(self, source_file):
        """
        Infers the schema of the file where the header is not present.
        This goes through the file to check the max number of delimters per row. The
        reason for this is that some files come with varying numbers of delimiters in
        each row of the same file. Spark/Pandas will use the first row as the default
        number of columns meaning subsequent rows can lose data.
        These columns are then iterated to create and return a spark schema.
        """
        data = self.spark_session.read.text(source_file)
        data.repartition(120)

        delimiter = self.file_options["sep"]
        # Get max number of delimiters and therefore columns from the file
        columns = (
            data.withColumn(
                "no_of_seps",
                functions.length(
                    functions.regexp_replace("value", f"[^{delimiter}]", "")
                ),
            )
            .groupby()
            .max("no_of_seps")
            .collect()[0]
            .asDict()["max(no_of_seps)"]
            + 1
        )

        # Create spark schema from the range of columns.
        fields = [
            StructField(f"_c{index}", StringType(), True) for index in range(columns)
        ]

        return StructType(fields)

    def add_row_index(self, dataframe):
        """
        Adds a sequential row index (__row_id) to the dataframe using the
        rdd.zipWithIndex functionality.
        """

        if self.includes_row_index and len(dataframe.schema.names) > 0:
            last_column = dataframe.schema.names[-1]
            return dataframe.withColumnRenamed(last_column, "__row_id")

        # Create schema from existing, adding a new column for the row_id
        schema = StructType(
            [StructField("__row_id", LongType(), False)] + dataframe.schema.fields[:]
        )

        return (
            dataframe.rdd.zipWithIndex()
            .map(lambda row: (row[1],) + tuple(row[0]))
            .toDF(schema)
        )

    def _read_file(self):
        """
        Reads the file into a spark dataframe, inferring header where needed.
        """
        logger.info(
            json_log(
                "Reading File for data quality",
                FileID=self.file_id,
                FileName=self.source_path,
            )
        )
        source_file = f"{self.source_container.container_url}{self.source_path}"

        # We need to remove the multiline option as this can have issues with json
        # files that have already been through spark.
        amended_file_options = self.file_options.copy()
        if self.file_type == "json":
            amended_file_options.pop("multiline", None)

        # Create reader that will load the file
        spark_reader = self.spark_session.read.format(self.file_type).options(
            **amended_file_options
        )

        dataframe = spark_reader.load(source_file)
        dataframe = self.add_row_index(dataframe)

        return dataframe.repartition(4)

    def numeric_check(self, column, numeric_type):
        logger.info(
            json_log(
                "Running numeric data quality check for column",
                FileID=self.file_id,
                FileName=self.source_path,
                ColumnName=column,
            )
        )
        # Spark doesn't have decimal and instead uses double
        numeric_type = numeric_type.lower().replace("decimal", "double")
        return self.data.where(
            (self.data[f"`{column}`"].cast(numeric_type).isNull())
            & (self.data[f"`{column}`"].isNotNull())
        ).select("__row_id", f"`{column}`")

    def datetime_check(self, column, date_format=None):
        logger.info(
            json_log(
                "Running datetime data quality check for column",
                FileID=self.file_id,
                FileName=self.source_path,
                ColumnName=column,
                DateFormat=date_format,
            )
        )
        self.spark_session.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
        return self.data.where(
            (functions.to_timestamp(self.data[f"`{column}`"], date_format).isNull())
            & (self.data[f"`{column}`"].isNotNull())
        ).select("__row_id", f"`{column}`")

    def null_check(self, column):
        logger.info(
            json_log(
                "Running null data quality check for column",
                FileID=self.file_id,
                FileName=self.source_path,
                ColumnName=column,
            )
        )
        return self.data.where(self.data[f"`{column}`"].isNull()).select(
            "__row_id", f"`{column}`"
        )

    def append_errors(self, errors, column, rule, date_format):
        if errors.count() == 0:
            return
        errors_df = errors.select(
            functions.lit(f'{rule}{date_format if date_format else ""}').alias("rule"),
            functions.col("__row_id").alias("row_index").cast(LongType()),
            functions.lit(column).alias("column"),
            functions.col(f"`{column}`").alias("value"),
        )

        logger.warning(
            json_log(
                "Data issues found",
                FileID=self.file_id,
                FileName=self.source_path,
                ColumnName=column,
                Rule=f'{rule}{date_format if date_format else ""}',
                Rows=errors_df.count(),
            )
        )
        self.errors = self.errors.union(errors_df)

    def run_data_quality_check(self, rule, column, date_format=None):
        if rule in NUMERIC_TYPES:
            errors = self.numeric_check(column, rule)
        elif rule == "datetime":
            errors = self.datetime_check(column, date_format)
        elif rule == "nullable":
            errors = self.null_check(column)
        else:
            return
        self.append_errors(errors, column, rule, date_format)

    def run_column_data_checks(self):
        logger.info(
            json_log(
                "Running column data quality checks",
                FileID=self.file_id,
                FileName=self.source_path,
            )
        )
        for column in self.schema.details:
            if column.ignore:
                continue
            col_name = column.file_column
            if col_name in self.missing_columns:
                continue
            self.run_data_quality_check(
                column.dtype, col_name, date_format=column.date_format
            )
            if not column.nullable:
                self.run_data_quality_check("nullable", col_name)

    def summarise_errors(self):
        """
        Updates the summary to include details of the errors. This will iterate over
        all of the found errors and produce a dictionary summary of each column and rule
        and the errors found with that.
        """
        self._summary["total_rows"] = self.data.count()
        self._summary["invalid_rows"] = self.error_unique_row_count
        self._summary["quality_state"] = self.quality_state
        if self.missing_columns:
            self._summary["missing_columns"] = self.missing_columns
            self._summary["invalid_rows"] = self._summary["total_rows"]
        if self.errors.count() == 0:
            return
        self._summary["errors"] = [
            *map(
                lambda row: row.asDict(),
                self.errors.select("rule", "column")
                .distinct()
                .sort(["column", "rule"])
                .collect(),
            )
        ]
        for index, column_rule in enumerate(self._summary["errors"]):
            rule = column_rule["rule"]
            column = column_rule["column"]
            # ignore value for nullable rules as always null
            column_list = (
                ["row_index"] if rule == "nullable" else ["row_index", "value"]
            )
            # Get errors for a particular rule
            column_rule_errors = (
                self.errors.where(
                    (functions.col("rule") == rule)
                    & (functions.col("column") == column)
                )
                .select(*column_list)
                .sort("row_index")
            )
            self._summary["errors"][index]["rows"] = [
                *map(lambda row: row.asDict(), column_rule_errors.collect())
            ]

    def save_summary(self):
        self.output_container.container.write_file(
            file_name=self.output_file,
            content=BytesIO(json.dumps(self.summary).encode("utf-8")),
        )

    def profile_data(self):
        logger.info(
            json_log(
                "Data profiling has been disabled",
                FileID=self.file_id,
                FileName=self.source_path,
            )
        )
        return
        if self.data.count() == 0:
            logger.info(
                json_log(
                    "Empty data file so no data profiling was run",
                    FileID=self.file_id,
                    FileName=self.source_path,
                )
            )
            return
        profile = ProfileReport(
            self.data.toPandas().dropna(axis=1, how="all"),
            title=self.source_path,
            minimal=True,
        )

        logger.info(
            json_log(
                "Creating json data profiling report",
                FileID=self.file_id,
                FileName=self.source_path,
                DestinationFile=f"{self.output_profiling_file}.json",
            )
        )
        json_profile = profile.to_json()
        self.output_container.container.write_file(
            file_name=f"{self.output_profiling_file}.json",
            content=BytesIO(json_profile.encode("utf-8")),
        )

        logger.info(
            json_log(
                "Creating html data profiling report",
                FileID=self.file_id,
                FileName=self.source_path,
                DestinationFile=f"{self.output_profiling_file}.html",
            )
        )
        html_profile = profile.to_html()
        self.output_container.container.write_file(
            file_name=f"{self.output_profiling_file}.html",
            content=BytesIO(html_profile.encode("utf-8")),
        )

    def run(self):
        self.run_column_data_checks()
        self.summarise_errors()
        self.save_summary()
        self.profile_data()
        if self.quality_state == "ok":
            logger.info(
                json_log(
                    "Data Quality check complete with no issues",
                    FileID=self.file_id,
                    FileName=self.source_path,
                    **self.condensed_summary,
                )
            )
        else:
            logger.warning(
                json_log(
                    "Data Quality check has found some issues and failed this file",
                    FileID=self.file_id,
                    FileName=self.source_path,
                    **self.condensed_summary,
                )
            )
        return self.condensed_summary