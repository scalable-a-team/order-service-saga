import uuid as uuid
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy import Integer, Column, String, Numeric, DateTime, func, event, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# Django already handle table migration/creation
class Order(Base):
    __tablename__ = 'order_order'
    uuid = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    status = Column(String)
    seller_id = Column(UUID(as_uuid=True), nullable=True)
    buyer_id = Column(UUID(as_uuid=True))
    product_id = Column(Integer)
    total_incl_tax = Column(Numeric(precision=12, scale=2), nullable=True)
    created_at = Column(DateTime(timezone=True), default=func.now())
    updated_at = Column(DateTime(timezone=True), default=func.now())
    dimension_text = Column(String)
    description_text = Column(String)


class ProcessedEvent(Base):
    __tablename__ = 'processed_event'
    event_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    chain_id = Column(UUID(as_uuid=True), default=uuid.uuid4)
    event = Column(String(32))
    next_event = Column(String(32), nullable=True)
    step = Column(Integer)

    __table_args__ = (
        UniqueConstraint('chain_id', 'event', name='chain_event_uc'),
    )


def update_time_modifier(mapper, connection, target):
    target.updated_at = func.now()


event.listen(
    Order,
    'before_update',
    update_time_modifier
)
