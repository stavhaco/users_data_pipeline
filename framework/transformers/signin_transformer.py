from pyspark.sql.functions import col, explode
from .base import DataTransformer

class SignInTransformer(DataTransformer):
    def transform(self, df):
        df = df.select(explode(col("value")).alias("user_data"))
        signin_data = df.select(
            col("user_data.id").alias("id"),
            col("user_data.signInActivity.lastSignInDateTime").alias("last_sign_in_date_time"),
            col("user_data.signInActivity.lastSignInRequestId").alias("last_sign_in_request_id"),
            col("user_data.signInActivity.lastNonInteractiveSignInDateTime").alias("last_non_interactive_sign_in_date_time"),
            col("user_data.signInActivity.lastNonInteractiveSignInRequestId").alias("last_non_interactive_sign_in_request_id"),
            col("user_data.signInActivity.lastSuccessfulSignInDateTime").alias("last_successful_sign_in_date_time"),
            col("user_data.signInActivity.lastSuccessfulSignInRequestId").alias("last_successful_sign_in_request_id")
        )
        return signin_data 