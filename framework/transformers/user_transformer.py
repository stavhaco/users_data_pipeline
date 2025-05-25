from pyspark.sql.functions import col, when, explode
from .base import DataTransformer

class UserTransformer(DataTransformer):
    def transform(self, df):
        df = df.select(explode(col("value")).alias("user_data"))
        user_data = df.select(
            col("user_data.id").alias("id"),
            col("user_data.userPrincipalName").alias("user_principal_name"),
            when(col("user_data.mail").isNull(), col("user_data.otherMails")[0]).otherwise(col("user_data.mail")).alias("mail"),
            col("user_data.userType").alias("user_type"),
            col("user_data.usageLocation").alias("location"),
            col("user_data.accountEnabled").alias("is_enabled"),
            col("user_data.givenName").alias("first_name"),
            col("user_data.surname").alias("last_name")
        )
        return user_data 