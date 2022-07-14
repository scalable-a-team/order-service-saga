import os

from celery import Celery
from celery.signals import worker_process_init
from celery.utils.log import get_task_logger
from opentelemetry.instrumentation.celery import CeleryInstrumentor
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, and_

import celeryconfig
from enums import OrderStatus, EventStatus
from models import Order, ProcessedEvent

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
engine = create_engine(
    f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}',
    echo=True
)
Session = sessionmaker(bind=engine)

try:
    ProcessedEvent.__table__.create(engine)
except:
    pass


@worker_process_init.connect(weak=False)
def init_celery_tracing(*args, **kwargs):
    CeleryInstrumentor().instrument()


app = Celery()
app.config_from_object(celeryconfig)
logger = get_task_logger(__name__)


@app.task(name=EventStatus.CREATE_ORDER, bind=True)
def create_order(self, buyer_id, product_id, order_id):
    self.update_state(state='STARTED')
    logger.info(f"Receive Buyer ID: {buyer_id}, Product ID: {product_id}, Order ID: {order_id}")
    db_session = Session()

    event_record = db_session.query(ProcessedEvent).filter(and_(
        ProcessedEvent.chain_id == order_id,
        ProcessedEvent.event == EventStatus.CREATE_ORDER,
    )).first()
    db_session.commit()

    payload = {'order_id': order_id, 'product_id': product_id, 'buyer_id': buyer_id, 'step': 0}
    # If event is already processed, we skip the event processing
    # but fire the next event just in-case the next-published message is lost
    if event_record is not None:
        app.send_task(
            event_record.next_event,
            kwargs=payload,
            queue=EventStatus.get_queue(event_record.next_event),
        )
        return payload

    order = Order(
        uuid=order_id,
        status=OrderStatus.INIT,
        buyer_id=buyer_id,
        product_id=product_id,
    )
    history = ProcessedEvent(
        chain_id=order_id,
        event_id=self.request.id,
        event=EventStatus.CREATE_ORDER,
        next_event=EventStatus.UPDATE_PRODUCT_QUOTA,
        step=0
    )

    transaction_success = False

    try:
        with db_session.begin():
            db_session.add_all([order, history])
        transaction_success = True
    except Exception as e:
        logger.error(e)
        logger.info(f"{EventStatus.CREATE_ORDER} failed for Buyer ID: {buyer_id} Product ID: {product_id}")
    # Since this is the origin of SAGA, no need to revert event when transaction failed
    if transaction_success:
        app.send_task(
            EventStatus.UPDATE_PRODUCT_QUOTA,
            kwargs=payload,
            queue=EventStatus.get_queue(EventStatus.UPDATE_PRODUCT_QUOTA),
        )
    return payload


@app.task(name=EventStatus.REVERT_CREATE_ORDER, bind=True)
def revert_create_order(self, order_uuid):
    # TODO: Work on remove order
    self.update_state(state='STARTED')
    return True