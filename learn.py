from fastapi import FastAPI
from fastapi.params import Body
from pydantic import BaseModel
from enum import Enum

app1 = FastAPI()
class BlogType(str,Enum):
    short:'short'
    story:'story'
    howto:'howto'
@app1.get('/blog/type/{type}')
def get_blog_type(type:BlogType):
    return {"message":f"BlogType:{type}"}
    