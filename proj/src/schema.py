from pydantic import BaseModel, Field
from typing import Literal
class InputDataSchema(BaseModel):
    id: str = Field(
      ...,
      description="Unique ID, should be included in the results"
    )
    numberOfChildren: int = Field(
      ...,
      ge=0,
      description="Number of children, must be >= 0"
    )
    familyComposition: Literal["single", "couple"] = Field(
      ...,
      description='Choices are "single" or "couple"'
    )
    familyUnitInPayForDecember: bool = Field(
      ...,
      description="Used for eligibility determination"
    )

    class Config:
        strict = True