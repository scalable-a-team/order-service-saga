class OrderStatus:
    INIT = 'init'
    PENDING = 'pending'
    IN_PROGRESS = 'in_progress'
    SUCCESS = 'success'


class QueueName:
    ORDER = 'order'
    PRODUCT = 'product'


class EventStatus:
    CREATE_ORDER = 'create_order'
    REVERT_CREATE_ORDER = 'revert_create_order'
    UPDATE_PRODUCT_QUOTA = 'update_product_quota'
    RESERVE_BUYER_CREDIT = 'reserve_buyer_wallet'

    _queue_mapping = {
        UPDATE_PRODUCT_QUOTA: 'product',
        RESERVE_BUYER_CREDIT: 'user'
    }

    @classmethod
    def get_queue(cls, name):
        return cls._queue_mapping[name]
