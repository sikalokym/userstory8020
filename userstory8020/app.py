import logging
from contextlib import contextmanager
from csv import Error, Sniffer
from os import R_OK, access
from pathlib import Path
from typing import Dict, List, Optional, Union

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.connect.dataframe import DataFrame as DataFrameCon
from pyspark.sql.functions import col

app_logger = logging.getLogger(__name__)


@contextmanager
def open_spark_session(spark_remote_url: str):
    spark_session = SparkSession.builder \
                                .remote(spark_remote_url) \
                                .getOrCreate()
    try:
        app_logger.info("Spark session is opened.")
        yield spark_session
    finally:
        spark_session.stop()
        app_logger.info("Spark session is closed.")


class DataFrameManipulation():
    def __init__(self, spark_sesion: SparkSession, path: str) -> None:
        #file_validation(filePath=path)
        self.__df = spark_sesion.read.csv(path,
                                          sep=',',
                                          header=True)

    @property
    def df(self) -> DataFrame:
        return self.__df

    @df.setter
    def df(self, df: DataFrame) -> None:
        if isinstance(df, DataFrameCon):
            self.__df = df

    def filter_rows(self,
                    filterConditions: Dict[str,
                                           Union[str,
                                                 List[str]]]):
        logging.info('Filtering data.')
        logging.debug(f'filterConditions - {filterConditions}')
        for col_name, col_value in filterConditions.items():
            self.__df = self.__df \
                            .filter(self.__df[col_name].isin(col_value))
        logging.info('Data is filtered.')

    def join_dataframe(self,
                       df: Union[DataFrame, DataFrameCon],
                       on: List[str]):
        self.__df = self.__df.join(other=df, on=on)

    def select_columns(self,
                       colsList: List[str],
                       colsMap: Optional[Dict[str, str]] = None) -> None:
        logging.info('Selecting columns.')
        logging.debug(f'colsList - {colsList}')
        if colsMap:
            self.rename_columns(colsMap=colsMap)
            colsList.extend(colsMap.values())
        self.__df = self.__df.select([c for c in self.__df.columns if c in colsList])
        logging.info('Columns selected.')

    def rename_columns(self,
                       colsMap: Dict[str, str]) -> None:
        logging.info('Renaming columns.')
        logging.debug(f'colsMap - {colsMap}')
        self.__df = self.__df.select([col(c).alias(colsMap.get(c, c)) for c in self.__df.columns])
        logging.info('Columns renamed.')

    def save_output(self, outputPath: str):
        logging.info('Saving output.')
        logging.debug(f'OutputPath - {outputPath}')
        self.__df.write \
                 .mode("overwrite") \
                 .option("header", True) \
                 .csv(outputPath)
        logging.info('Output saved.')


def file_validation(filePath: str) -> None:
    p = Path(filePath)
    if p.exists() and p.is_file():
        if access(filePath, R_OK):
            csv_validation(filePath)
        else:
            err_msg = 'No sufficient access to the file.'
            app_logger.error(err_msg)
            raise AppException(err_msg)
    else:
        err_msg = 'The file or directory does not exist!'
        app_logger.error(err_msg)
        raise AppException(err_msg)


def csv_validation(filePath: str):
    try:
        with open(filePath, newline='') as csvfile:
            start = csvfile.read(1024)
            Sniffer().sniff(start)
            if not Sniffer().has_header(start):
                app_logger.error("Seems to be the header is absent.")
                raise AppException(f'file: {filePath} is incorrect!')
    except Error as e:
        app_logger.error(e)
        raise AppException(f'file: {filePath} is incorrect!') from e


class AppException(Exception):
    pass
