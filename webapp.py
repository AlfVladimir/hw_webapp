
import databases
import sqlalchemy
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel
from datetime import datetime, timedelta


# Подключение к ДБ
DATABASE_URL = "postgresql://user_course:123321@localhost:5432/postgres"
#DATABASE_URL = "sqlite:///./test.db"

database = databases.Database(DATABASE_URL)

metadata = sqlalchemy.MetaData()


# SQL DDL
stores = sqlalchemy.Table(
    "stores",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("adress", sqlalchemy.String),
)

items = sqlalchemy.Table(
    "items",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("name", sqlalchemy.String),
    sqlalchemy.Column("price", sqlalchemy.Float),
)

sales = sqlalchemy.Table(
    "sales",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True, autoincrement=True),
    sqlalchemy.Column("sale_time", sqlalchemy.DateTime),
    sqlalchemy.Column("item_id",sqlalchemy.Integer,sqlalchemy.ForeignKey("items.id")),
    sqlalchemy.Column("store_id",sqlalchemy.Integer,sqlalchemy.ForeignKey("stores.id")),
)

# Запуск сервера

engine = sqlalchemy.create_engine(DATABASE_URL)

metadata.create_all(engine)

app = FastAPI()



@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


# Эндпоинты

#/stores/
class Store(BaseModel):
    id: int
    adress: str

@app.get("/stores/", response_model=list[Store])
async def read_stores():
    query = stores.select()
    return await database.fetch_all(query)


#/items/
class Item(BaseModel):
    id: int
    name: str
    price: float

@app.get("/items/", response_model=list[Item])
async def read_items():
    query = items.select()
    return await database.fetch_all(query)

#/items/top/
class ItemTop(BaseModel):
    id: int
    name: str
    sales_amount: int

@app.get("/items/top/", response_model=list[ItemTop])
async def read_items_top():
    query = sqlalchemy.select(
                            sales.c.item_id.label('id')
                            ,items.c.name.label('name')
                            ,sqlalchemy.func.count(items.c.price).label('sales_amount')
                        ).select_from(sales.join(items)
                        ).where(sales.c.sale_time>=datetime.now() - timedelta(days=30)
                        ).group_by(sales.c.item_id,items.c.name
                        ).order_by(sqlalchemy.func.count(items.c.price).desc()
                        ).limit(10)

    #print(query)
    return await database.fetch_all(query)


#/stores/top/
class StoreTop(BaseModel):
    id: int
    adress: str
    income: float

@app.get("/stores/top/", response_model=list[StoreTop])
async def read_stores_top():
    query = sqlalchemy.select(
                            stores.c.id.label('id')
                            ,stores.c.adress.label('adress')
                            ,sqlalchemy.func.sum(items.c.price).label('income')
                        ).select_from(sales.join(stores).join(items)
                        ).where(sales.c.sale_time>=datetime.now() - timedelta(days=30)
                        ).group_by(stores.c.id,stores.c.adress
                        ).order_by(sqlalchemy.func.sum(items.c.price).desc()
                        ).limit(10)

    #print(query)
    return await database.fetch_all(query)


# /sales/
class SaleOut(BaseModel):
    id: int
    sale_time: datetime
    item_id: int
    store_id: int

class SaleIn(BaseModel):
    item_id: int
    store_id: int

@app.post("/sales/", response_model=SaleOut)
async def create_sale(sale: SaleIn):
    if await database.execute(items.select().filter_by(id=sale.item_id)) is None:
        return JSONResponse(status_code=status.HTTP_400_BAD_REQUEST,
        content=jsonable_encoder({"error": "Нет такого товара"}),)
    if await database.execute(stores.select().filter_by(id=sale.store_id)) is None:
        return JSONResponse(status_code=status.HTTP_400_BAD_REQUEST,
        content=jsonable_encoder({"error": "Нет такого магазина"}),)        
    curtime = datetime.now()
    query = sales.insert().values(item_id=sale.item_id, store_id=sale.store_id, sale_time=curtime)
    last_record_id = await database.execute(query)
    return {"id": last_record_id, "sale_time": curtime, **sale.dict()}


# Обработка ошибок
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request,
    exc: RequestValidationError
):
    return JSONResponse(status_code=status.HTTP_400_BAD_REQUEST,
        content=jsonable_encoder({"error": "Не корректные данные"}),
    )



