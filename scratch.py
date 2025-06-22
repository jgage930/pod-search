from pydantic import BaseModel


class Test(BaseModel):
    a: int
    b: str


print(Test.model_fields)

for f, info in Test.model_fields.items():
    print(f)
    print(info.annotation)