import os

from celery import Celery
from celery.signals import worker_process_init
from celery.utils.log import get_task_logger
from opentelemetry import trace, propagate
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.celery import CeleryInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace import TracerProvider
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

ProcessedEvent.__table__.create(engine, checkfirst=True)

tracer = None
PROPAGATOR = None


@worker_process_init.connect(weak=False)
def init_celery_tracing(*args, **kwargs):
    global tracer
    global PROPAGATOR
    resource = Resource.create(attributes={
        "service.name": "OrderSagaWorker"
    })
    trace.set_tracer_provider(TracerProvider(resource=resource))
    span_processor = BatchSpanProcessor(
        OTLPSpanExporter(endpoint=os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT'), insecure=True)
    )
    trace.get_tracer_provider().add_span_processor(span_processor)
    SQLAlchemyInstrumentor().instrument(engine=engine)
    CeleryInstrumentor().instrument()
    tracer = trace.get_tracer(__name__)
    PROPAGATOR = propagate.get_global_textmap()


app = Celery()
app.config_from_object(celeryconfig)
logger = get_task_logger(__name__)


@app.task(name=EventStatus.CREATE_ORDER, bind=True)
def create_order(self, buyer_id, product_id, order_id, seller_id, product_amount, context_payload, job_description,
                 dimension):
    ctx = PROPAGATOR.extract(carrier=context_payload)
    current_event = EventStatus.CREATE_ORDER
    next_event = EventStatus.RESERVE_BUYER_CREDIT

    with tracer.start_as_current_span("SAGA create_order", context=ctx):
        with tracer.start_span(name="update task to STARTED status"):
            self.update_state(state='STARTED')
        logger.info(f"Receive Buyer ID: {buyer_id}, Product ID: {product_id}, Order ID: {order_id}")
        db_session = Session()

        event_record = db_session.query(ProcessedEvent).filter(and_(
            ProcessedEvent.chain_id == order_id,
            ProcessedEvent.event == current_event,
        )).first()
        db_session.commit()

        payload = {'order_id': order_id, 'product_id': product_id, 'buyer_id': buyer_id,
                   'seller_id': seller_id, 'product_amount': product_amount, 'context_payload': context_payload}
        # If event is already processed, we skip the event processing
        # but fire the next event just in-case the next-published message is lost
        if event_record is not None:
            logger.info(f"Receive duplicate event. chain_id {order_id}, event: {current_event}")
            with tracer.start_span(name=f"send_task {event_record.next_event}"):
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
            dimension_text=dimension,
            description_text=job_description
        )
        history = ProcessedEvent(
            chain_id=order_id,
            event_id=self.request.id,
            event=current_event,
            next_event=next_event,
            step=0
        )

        transaction_success = False

        with tracer.start_span(name="Execute DB Transaction"):
            try:
                with db_session.begin():
                    db_session.add_all([order, history])
                transaction_success = True
            except Exception as e:
                logger.error(e)
                logger.info(f"{current_event} failed for Buyer ID: {buyer_id} Product ID: {product_id}")

        # Since this is the origin of SAGA, no need to revert event when transaction failed
        if transaction_success:
            with tracer.start_span(name=f"send_task {next_event}"):
                app.send_task(
                    next_event,
                    kwargs=payload,
                    queue=EventStatus.get_queue(next_event),
                )
        return payload


@app.task(name=EventStatus.APPROVE_ORDER_PENDING, bind=True)
def approve_order_pending(self, buyer_id, product_id, order_id, seller_id, product_amount, context_payload):
    ctx = PROPAGATOR.extract(carrier=context_payload)
    current_event = EventStatus.APPROVE_ORDER_PENDING
    self.update_state(state='STARTED')
    with tracer.start_as_current_span(f"SAGA {current_event}", context=ctx):
        db_session = Session()

        event_record = db_session.query(ProcessedEvent).filter(and_(
            ProcessedEvent.chain_id == order_id,
            ProcessedEvent.event == current_event,
        )).first()
        db_session.commit()

        if event_record is not None:
            logger.info(f"Receive duplicate event. chain_id {order_id}, event: {current_event}")
            return

        with db_session.begin():
            order = db_session.query(Order).filter(Order.uuid == order_id).first()
            order.status = OrderStatus.PENDING
            order.total_incl_tax = product_amount
            order.seller_id = seller_id
            db_session.flush()
            history = ProcessedEvent(
                chain_id=order_id,
                event_id=self.request.id,
                event=current_event,
                next_event=None,
                step=0
            )
            db_session.add(history)


@app.task(name=EventStatus.REVERT_CREATE_ORDER, bind=True)
def revert_create_order(self, buyer_id, product_id, order_id, seller_id, product_amount, context_payload):
    ctx = PROPAGATOR.extract(carrier=context_payload)
    self.update_state(state='STARTED')
    current_event = EventStatus.REVERT_CREATE_ORDER
    with tracer.start_as_current_span(f"SAGA {current_event}", context=ctx):
        db_session = Session()

        event_record = db_session.query(ProcessedEvent).filter(and_(
            ProcessedEvent.chain_id == order_id,
            ProcessedEvent.event == current_event,
        )).first()
        db_session.commit()

        if event_record is not None:
            logger.info(f"Receive duplicate event. chain_id {order_id}, event: {current_event}")
            return

        with tracer.start_span(name="Execute DB Transaction"):
            try:
                with db_session.begin():
                    db_session.query(Order).filter(Order.uuid == order_id).delete()
                    history = ProcessedEvent(
                        chain_id=order_id,
                        event_id=self.request.id,
                        event=current_event,
                        next_event=None,
                        step=0
                    )
                    db_session.add(history)
            except Exception as e:
                logger.error(e)
                logger.info(f"{current_event} failed for Buyer ID: {buyer_id} Product ID: {product_id}")
                raise e


@app.task(name=EventStatus.UPDATE_ORDER_SUCCESS, bind=True)
def update_order_success(self, order_id, seller_id, product_amount, buyer_id, context_payload):
    ctx = PROPAGATOR.extract(carrier=context_payload)
    self.update_state(state='STARTED')
    current_event = EventStatus.UPDATE_ORDER_SUCCESS
    with tracer.start_as_current_span(f"SAGA {current_event}", context=ctx):
        db_session = Session()

        event_record = db_session.query(ProcessedEvent).filter(and_(
            ProcessedEvent.chain_id == order_id,
            ProcessedEvent.event == current_event,
        )).first()
        db_session.commit()

        payload = {'order_id': order_id, 'product_amount': product_amount, 'seller_id': seller_id,
                   'buyer_id': buyer_id, 'context_payload': context_payload}
        if event_record is not None:
            logger.info(f"Receive duplicate event. chain_id {order_id}, event: {current_event}")
            with tracer.start_span(name=f"send_task {event_record.next_event}"):
                app.send_task(
                    event_record.next_event,
                    kwargs=payload,
                    queue=EventStatus.get_queue(event_record.next_event),
                )
                return payload
            return

    next_event = EventStatus.TRANSFER_TO_SELLER_BALANCE
    with tracer.start_span(name="Execute DB Transaction"):
        try:
            with db_session.begin():
                order = db_session.query(Order).with_for_update().filter_by(uuid=order_id).first()
                if order.status != OrderStatus.PENDING:
                    raise Exception("Invalid order status in SAGA")
                order.status = OrderStatus.SUCCESS
                db_session.flush()
                history = ProcessedEvent(
                    chain_id=order_id,
                    event_id=self.request.id,
                    event=current_event,
                    next_event=next_event,
                    step=0
                )
                db_session.add(history)
        except Exception as e:
            logger.error(e)
            logger.info(f"{current_event} failed for Order ID: {order_id}")
            raise e

    with tracer.start_span(name=f"send_task {next_event}"):
        app.send_task(
            next_event,
            kwargs=payload,
            queue=EventStatus.get_queue(next_event),
        )


@app.task(name=EventStatus.UPDATE_ORDER_REJECTED, bind=True)
def update_order_rejected(self, order_id, buyer_id, product_amount, seller_id, context_payload):
    ctx = PROPAGATOR.extract(carrier=context_payload)
    self.update_state(state='STARTED')
    current_event = EventStatus.UPDATE_ORDER_REJECTED

    with tracer.start_as_current_span(f"SAGA {current_event}", context=ctx):
        db_session = Session()

        event_record = db_session.query(ProcessedEvent).filter(and_(
            ProcessedEvent.chain_id == order_id,
            ProcessedEvent.event == current_event,
        )).first()
        db_session.commit()

        payload = {'order_id': order_id, 'product_amount': product_amount, 'buyer_id': buyer_id,
                   'seller_id': seller_id, 'context_payload': context_payload}
        if event_record is not None:
            logger.info(f"Receive duplicate event. chain_id {order_id}, event: {current_event}")
            with tracer.start_span(name=f"send_task {event_record.next_event}"):
                app.send_task(
                    event_record.next_event,
                    kwargs=payload,
                    queue=EventStatus.get_queue(event_record.next_event),
                )
                return payload

    next_event = EventStatus.REFUND_BUYER
    with tracer.start_span(name="Execute DB Transaction"):
        try:
            with db_session.begin():
                order = db_session.query(Order).with_for_update().filter_by(uuid=order_id).first()
                if order.status != OrderStatus.PENDING:
                    raise Exception("Invalid order status in SAGA")
                order.status = OrderStatus.REJECTED
                db_session.flush()
                history = ProcessedEvent(
                    chain_id=order_id,
                    event_id=self.request.id,
                    event=current_event,
                    next_event=next_event,
                    step=0
                )
                db_session.add(history)
        except Exception as e:
            logger.error(e)
            logger.info(f"{current_event} failed for Order ID: {order_id}")
            raise e

    with tracer.start_span(name=f"send_task {next_event}"):
        app.send_task(
            next_event,
            kwargs=payload,
            queue=EventStatus.get_queue(next_event),
        )


def _header_from_carrier(carrier, key):
    header = carrier.get(key)
    return [header] if header else []
