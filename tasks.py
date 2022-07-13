import os

from celery import Celery
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, MetaData

import celeryconfig
from enums import OrderStatus
from models import Order


DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
engine = create_engine(
    f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}',
    echo=True
)
Session = sessionmaker(bind=engine)
db_session = Session()

# metadata_obj = MetaData()


app = Celery()
app.config_from_object(celeryconfig)


@app.task(name='create_order', bind=True)
def create_order(self, buyer_id, product_id):
    self.update_state(state='STARTED')
    order = Order(
        status=OrderStatus.INIT,
        buyer_id=buyer_id,
        product_id=product_id,
    )
    db_session.add(order)
    db_session.commit()

    return True


