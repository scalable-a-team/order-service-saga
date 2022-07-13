import os

from celery import Celery
from celery.utils.log import get_task_logger
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

import celeryconfig
from enums import OrderStatus, EventStatus, QueueName
from models import Order, EventHistory

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

try:
    EventHistory.__table__.create(engine)
except:
    pass

app = Celery()
app.config_from_object(celeryconfig)
logger = get_task_logger(__name__)


@app.task(name=EventStatus.CREATE_ORDER, bind=True)
def create_order(self, buyer_id, product_id):
    self.update_state(state='STARTED')
    logger.info(f"Receive Buyer ID: {buyer_id} Product ID: {product_id}")
    order = Order(
        status=OrderStatus.INIT,
        buyer_id=buyer_id,
        product_id=product_id,
    )
    uuid = order.uuid
    history = EventHistory(
        chain_id=uuid,
        event_id=self.request.id,
        event=EventStatus.CREATE_ORDER,
        step=0
    )
    payload = {'order_id': str(order.uuid), 'product_id': product_id, 'buyer_id': buyer_id, 'step': 0}
    transaction_success = False

    try:
        with db_session.begin():
            db_session.add_all([order, history])
        transaction_success = True
    except Exception as e:
        logger.error(e)
        logger.info(f"{EventStatus.CREATE_ORDER} failed for Buyer ID: {buyer_id} Product ID: {product_id}")

    if transaction_success:
        app.send_task(
            EventStatus.UPDATE_PRODUCT_QUOTA,
            kwargs=payload,
            queue=QueueName.PRODUCT,
        )
    return payload


@app.task(name=EventStatus.REVERT_CREATE_ORDER, bind=True)
def revert_create_order(self, order_uuid):
    # TODO: Work on remove order
    self.update_state(state='STARTED')
    return True