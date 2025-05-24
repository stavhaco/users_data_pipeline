import logging
from framework.readers.spark_json_reader import SparkJsonReader
from framework.transformers.user_transformer import UserTransformer
from framework.transformers.signin_transformer import SignInTransformer
from framework.writers.spark_json_writer import SparkJsonWriter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataPipeline:
    def __init__(self, reader, user_transformer, signin_transformer, writer):
        self.reader = reader
        self.user_transformer = user_transformer
        self.signin_transformer = signin_transformer
        self.writer = writer

    def run(self, input_path, output_paths):
        df = self.reader.read(input_path)
        user_df = self.user_transformer.transform(df)
        signin_df = self.signin_transformer.transform(df)
        self.writer.write(user_df, output_paths['user'])
        self.writer.write(signin_df, output_paths['signin'])
        logger.info("Pipeline complete.")

def main():
    # NOTE: You can swap out these components for other formats or logic.
    reader = SparkJsonReader()
    user_transformer = UserTransformer()
    signin_transformer = SignInTransformer()
    writer = SparkJsonWriter()

    input_path = "input/"
    output_paths = {
        'user': "output/user/",
        'signin': "output/signin/"
    }

    logger.info("handeling input data from %s", input_path)
    df = reader.read(input_path)
    user_df = user_transformer.transform(df)
    signin_df = signin_transformer.transform(df)

    # Write outputs (mode can be 'overwrite' or 'append')
    logger.info("Writing user data to %s", output_paths['user'])
    writer.write(user_df, output_paths['user'], mode='overwrite')
    logger.info("Writing sign-in data to %s", output_paths['signin'])
    writer.write(signin_df, output_paths['signin'], mode='overwrite')
    logger.info("Pipeline complete.")

if __name__ == "__main__":
    main() 