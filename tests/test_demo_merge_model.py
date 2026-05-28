from typing import Annotated, Literal

from pydantic import BaseModel, Field, TypeAdapter


class BigQueryCount(BaseModel):
    uses: Literal["bigquery_count"]
    query: str


class CloudStorageWithPrefix(BaseModel):
    uses: Literal["cloud_storage"]
    prefix: str


class Empty(BaseModel):
    uses: Literal["empty"]


class Smooth(BaseModel):
    uses: Literal["smooth"]
    factor: int


Task = Annotated[
    BigQueryCount | CloudStorageWithPrefix,
    Field(discriminator="uses"),
]


Task2 = Annotated[
    Empty | Smooth,
    Field(discriminator="uses"),
]


CombinedTask = Annotated[
    Task.__args__[0] | Task2.__args__[0],
    Field(discriminator="uses"),
]


def test_merge_model():
    bq = TypeAdapter(CombinedTask).validate_python(
        {
            "uses": "bigquery_count",
            "query": "SELECT COUNT(*) FROM my_table",
        }
    )
    print(bq)
