import argparse
import json
import logging
from typing import Dict

from .app import AppException, open_spark_session

main_logger = logging.getLogger(__name__)


def main():
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("-sp", "--sparkremoteurl", required=True)
        parser.add_argument("-prc", "--processfile", required=True)
        args = parser.parse_args()

        dataframes_lst: Dict[str, object] = {}

        with open(args.processfile) as f:
            process = json.load(f)

        with open_spark_session(spark_remote_url=args.sparkremoteurl) as spark:
            stream = process.get('startstream')
            cls: object = None
            while True:
                extract = next((item for item in process.get('extracts')
                                if item['stream'] == stream), None)
                if extract:
                    module_path = ".".join(extract['callable'].split(".")[:-1])
                    module = __import__(module_path, fromlist=[None])
                    cls_name = extract['callable'].split(".")[-1:][0]
                    cls = getattr(module, cls_name)
                    dataframes_lst[extract['dataframe']] = \
                        cls(spark, **extract['params'])
                    stream = extract.get('downstream', {})
                    continue

                transform = next((item for item in process.get('transforms')
                                  if item['stream'] == stream), None)
                if transform:
                    fun = getattr(dataframes_lst[transform['dataframe']],
                                  transform['callable'])
                    fun(**transform['params'])
                    stream = transform.get('downstream', {})
                    continue

                join = next((item for item in process.get('joins')
                             if item['stream'] == stream), None)
                if join:
                    df = getattr(dataframes_lst[join['target']],
                                 "df")
                    fun = getattr(dataframes_lst[join['source']],
                                  join['callable'])
                    fun(df, **join.get('params', {}))
                    stream = join.get('downstream', {})
                    continue

                load = next((item for item in process.get('loads')
                             if item['stream'] == stream), None)
                if load:
                    fun = getattr(dataframes_lst[load['dataframe']],
                                  load['callable'])
                    fun(**load.get('params', {}))
                    stream = load.get('downstream', {})
                    dataframes_lst[load['dataframe']].df.show()
                    continue
                break

    except AppException as e:
        main_logger.error(e)
    except Exception as e:
        main_logger.error(e)
        main_logger.info("Programm completed with an ERROR!")
    else:
        main_logger.info("Programm completed SUCCESSFULLY!")


if __name__ == "__main__":
    main()
