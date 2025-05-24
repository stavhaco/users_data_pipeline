import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType
from framework.transformers.user_transformer import UserTransformer

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("Test").getOrCreate()

def make_df(spark, user_data):
    user_schema = StructType([
        StructField("id", StringType(), True),
        StructField("userPrincipalName", StringType(), True),
        StructField("mail", StringType(), True),
        StructField("otherMails", ArrayType(StringType()), True),
        StructField("userType", StringType(), True),
        StructField("usageLocation", StringType(), True),
        StructField("accountEnabled", BooleanType(), True),
        StructField("givenName", StringType(), True),
        StructField("surname", StringType(), True),
    ])
    from pyspark.sql import Row
    return spark.createDataFrame([Row(value=user_data)], schema=StructType([StructField("value", ArrayType(user_schema), True)]))

def test_mail_fallback_to_other_mails(spark):
    row = {
        "id": "1",
        "userPrincipalName": "user1@example.com",
        "mail": None,
        "otherMails": ["fallback@example.com"],
        "userType": "Member",
        "usageLocation": "US",
        "accountEnabled": True,
        "givenName": "Test",
        "surname": "User"
    }
    df = make_df(spark, [row])
    transformer = UserTransformer()
    user_df = transformer.transform(df)
    result = user_df.collect()[0].asDict()
    assert result["mail"] == "fallback@example.com"

def test_user_transformer_sanity(spark):
    row = {
        "id": "2",
        "userPrincipalName": "user2@example.com",
        "mail": "user2@mail.com",
        "otherMails": ["other2@mail.com"],
        "userType": "Guest",
        "usageLocation": "IL",
        "accountEnabled": False,
        "givenName": "Alice",
        "surname": "Smith"
    }
    df = make_df(spark, [row])
    transformer = UserTransformer()
    user_df = transformer.transform(df)
    result = user_df.collect()[0].asDict()
    assert result["mail"] == "user2@mail.com"
    assert result["external_id"] == "user2@example.com"
    assert result["type"] == "Guest"
    assert result["location"] == "IL"
    assert result["is_enabled"] is False
    assert result["first_name"] == "Alice"
    assert result["last_name"] == "Smith"

def test_all_fields_missing_or_null(spark):
    row = {
        "id": None,
        "userPrincipalName": None,
        "mail": None,
        "otherMails": None,
        "userType": None,
        "usageLocation": None,
        "accountEnabled": None,
        "givenName": None,
        "surname": None
    }
    df = make_df(spark, [row])
    transformer = UserTransformer()
    user_df = transformer.transform(df)
    result = user_df.collect()[0].asDict()
    assert all(v is None for v in result.values())

def test_other_mails_not_a_list(spark):
    row = {
        "id": "3",
        "userPrincipalName": "user3@example.com",
        "mail": None,
        "otherMails": None,  # Not a list
        "userType": "Member",
        "usageLocation": "US",
        "accountEnabled": True,
        "givenName": "Edge",
        "surname": "Case"
    }
    df = make_df(spark, [row])
    transformer = UserTransformer()
    user_df = transformer.transform(df)
    result = user_df.collect()[0].asDict()
    assert result["mail"] is None
