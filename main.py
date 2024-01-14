from fastapi import FastAPI,status,Response
from router import blog_get
from router import blog_post
from fastapi.params import Body
from pydantic import BaseModel
from enum import Enum
from typing import Optional
from sqlalchemy import create_engine 
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from db import models
from db.database import engine
from db iport models

app = FastAPI()
app.include_router(blog_get.router)
app.include_router(blog_get.router)

# to define the schema which user input , does validation for us

class Post(BaseModel):
    title: str
    content:str #content should be included and it should be string type
    publish:bool =True#a post should be published or not, here true is default value


@app.get("/")
async def root():
    return {"message":"Help world"}

@app.get("/hello")
async def root():
    return {"message":"Hello world"}

models.Base.metadata.create_all(engine)

@app.post("/createpost")
def create_post():
    return {"message":"return successfully"}

@app.post("/ccreatepost2")
def create_posts(user: dict = Body(...)):
    print(user)#this user is containing all things we give as input in postman body in json format
    return {"message": "succesfully created posts"}
@app.post("/newcreatepost")
def create_post(newpost: Post):
    print(newpost)
    print(newpost.title)
    print('$$$$$$$$$$$$$$$$$$$$$$4')
    #return {"title": newpost.title, "published": True, "summary": "Some summary of the content"}
    return {"data":"newpost"}

@app.get('/blog/{id}')
def get_blog(id):
    return {"message":f"hello blog with {id}"}


class BlogType(str,Enum):
    short = 'short'
    story = 'story'
    howto = 'howto'
@app.get('/blog/type/{type}')
def get_blog_type(type: BlogType):
    return {"message":f"BlogType:{type}"}
@app.get('/boball')
def get_all(page=1,page_size:Optional[int]=None):
    return{'message':f'All{page_size} blob on page{page}'}
@app.get('blob/all/{id}',status_code=status.HTTP_200_OK)#not working
def get_blob(id:int,response:Response):
    if id>5:
        response.status_code = status.HTTP_404_NOT_FOUND
        return{"error":f"response not found"}
    else:
        response.status_code = status.HTTP_200_OK
        return {"message":f'blog with id {id}'}

@app.get('blog/all',
tags=['blog'])

@app.get('commenting_tagging/{id}/{comment_id}',tags=['blog','comment'])
def add_comment(id:int,comment_id:int,valid:bool=True,username:Optional[str]=None):
    """
    stimulate retrieving of a comment of a blog
    **id** mandatory comment
    **comment_id** mandatory comment
    **valid** optional query parameter
    **username** optional query parameter
    """
    return {"message":f"{id},{comment_id},{valid},{username}"}