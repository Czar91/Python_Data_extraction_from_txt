# to start server : uvicorn app:app --reload
# in the browser : http://127.0.0.1:8000/gemi

import pandas as pd
from fastapi import FastAPI
from sqlalchemy import create_engine


engine = create_engine('mysql+mysqldb://root:Final_Root_Pass123@localhost/pythontest',pool_pre_ping=True)
dbConnection = engine.connect()
app = FastAPI()

@app.get("/{gemi}")
async def root(gemi):
    query=f"SELECT web,gemi,date,name FROM companies WHERE gemi={gemi}"
    # return pd.read_sql(query,engine)
    return pd.read_sql(query,engine).to_dict()