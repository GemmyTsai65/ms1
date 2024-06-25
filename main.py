from fastapi import FastAPI
from starlette.middleware import Middleware
from fastapi.middleware.cors import CORSMiddleware  # <- 新增這行
from prometheus_fastapi_instrumentator import Instrumentator
from fastapi import HTTPException
from pydantic import BaseModel
from typing import Optional
import logging
from contextlib import asynccontextmanager
import pyodbc
import uvicorn

app = FastAPI()

# 跨網域設定
origins = [
    "http://localhost:8000",
    "http://localhost",
    "http://192.168.10.5:8000",
    "http://192.168.10.5",
    "http://ms1.thi.com.tw:8000",
    "http://ms1.thi.com.tw",
    # 你可以加入其他的網域
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

instrumentator = Instrumentator().instrument(app).expose(app, include_in_schema=False)

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    # You can place cleanup code here if needed

app.router.lifespan = lifespan


# 数据库连接配置
db_server = "192.168.10.5"
db_name = "athitube"
db_username = "sa"
db_password = "thipm"

db_serverP = "192.168.10.5"
db_nameP = "PM"
db_usernameP = "sa"
db_passwordP = "thipm"

# 建立数据库连接
connection_string = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={db_server};DATABASE={db_name};UID={db_username};PWD={db_password}"
cnxn = pyodbc.connect(connection_string)
cursor = cnxn.cursor()

# 建立数据库连接PM
connection_stringP = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={db_serverP};DATABASE={db_nameP};UID={db_usernameP};PWD={db_passwordP}"
cnxnP = pyodbc.connect(connection_stringP)
cursorP = cnxnP.cursor()

# 数据表模型
class LikeData(BaseModel):
    id: int
    emp_id: str

class Video(BaseModel):
    id: int
    view_count: Optional[int]

# 取得同仁部門
def get_dep_id(emp_id: str) -> str:
    queryP = f"SELECT dep_id FROM emp WHERE emp_id = '{emp_id}'"
    cursorP.execute(queryP)
    dep_id = cursorP.fetchone()[0]
    return dep_id 

# 取得影片部門
def get_vedio_id(id: int) -> str:
    query = f"SELECT dep_id FROM videoDB WHERE id = {id}"
    cursor.execute(query)
    vdep_id = cursor.fetchone()[0]
    return vdep_id 

# 检查是否按讚存在记录
def check_record(id: int, emp_id: str) -> bool:
    query = f"SELECT COUNT(*) FROM likeDB WHERE id = {id} AND emp_id = '{emp_id}'"
    cursor.execute(query)
    count = cursor.fetchone()[0]
    return count > 0

# 检查是否觀看存在记录
def check_record_view(id: int, emp_id: str) -> bool:
    query = f"SELECT COUNT(*) FROM ViewDB WHERE id = {id} AND emp_id = '{emp_id}'"
    cursor.execute(query)
    count = cursor.fetchone()[0]
    return count > 0

# 添加新记录
def add_record_and_return_likes(id: int, emp_id: str) -> int:
    dep_id = get_dep_id(emp_id) 
    query = f"INSERT INTO likeDB (id, emp_id, dep_id, Ldate) VALUES ({id}, '{emp_id}','{dep_id}', GETDATE())"
    cursor.execute(query)
    vdep_id = get_vedio_id(id)
    if dep_id == vdep_id:
        query = f"Update VideoDB SET like_self = like_self+1 WHERE id = {id}"
        cursor.execute(query)   
        cnxn.commit()     
    else:
        query = f"Update VideoDB SET like_other = like_other+1 WHERE id = {id}"
        cursor.execute(query)   
        cnxn.commit()  

    query = f"Update VideoDB SET like_num = like_num+1 WHERE id = {id}"
    cursor.execute(query)   
    cnxn.commit()
    query = f"SELECT like_num FROM VideoDB WHERE id = {id}"
    cursor.execute(query)
    new_like_num = cursor.fetchone()[0]
    return new_like_num


# API 路由
@app.post("/likeDB/")
def like_data(like_data: LikeData):
    exists = check_record(like_data.id, like_data.emp_id)
    if exists:
        return {"message": "已按讚"}
    else:
        new_like_num = add_record_and_return_likes(like_data.id, like_data.emp_id)
        return {"message": "感謝您的支持", "like_num": new_like_num}

# 統計影片觀看人數
@app.get("/video/{video_id}/{emp_id}", response_model=Video)
async def update_video_view_count(video_id: int, emp_id: str):
    exists = check_record_view(video_id, emp_id)
    if exists:
        raise HTTPException(status_code=204, detail="View record already exists.") 
    else:        
        dep_id = get_dep_id(emp_id) 
        # Increase view count
        query = f"INSERT INTO ViewDB (id, emp_id,dep_id, Vdate) VALUES ({video_id}, '{emp_id}', '{dep_id}', GETDATE())"
        cursor.execute(query)

        dep_id = get_dep_id(emp_id) 
        vdep_id = get_vedio_id(video_id)
        if dep_id == vdep_id:
            query = f"Update VideoDB SET view_self = view_self+1 WHERE id = {video_id}"
            cursor.execute(query)   
            cnxn.commit()     
        else:
            query = f"Update VideoDB SET view_other = view_other+1 WHERE id = {video_id}"
            cursor.execute(query)   
            cnxn.commit()     

        cursor.execute(f"""
            UPDATE VideoDB
            SET view_num = view_num + 1
            WHERE id = {video_id};
        """)

        cnxn.commit()

        # Get the new view count
        cursor.execute(f"""
            SELECT view_num
            FROM VideoDB
            WHERE id = {video_id};
        """)

        row = cursor.fetchone()
        if row:
            return Video(id=video_id, view_count=row[0])
        else:
            raise HTTPException(status_code=404, detail="Video not found")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
