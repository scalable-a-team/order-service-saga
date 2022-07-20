class OrderStatus:
    INIT = 'init'
    PENDING = 'pending'
    IN_PROGRESS = 'in_progress'
    SUCCESS = 'success'
    REJECTED = 'rejected'


class QueueName:
    ORDER = 'order'
    PRODUCT = 'product'


class EventStatus:
    CREATE_ORDER = 'create_order'
    REVERT_CREATE_ORDER = 'revert_create_order'
    UPDATE_PRODUCT_QUOTA = 'update_product_quota'
    RESERVE_BUYER_CREDIT = 'reserve_buyer_wallet'
    APPROVE_ORDER_PENDING = 'approve_order_pending'
    UPDATE_ORDER_SUCCESS = 'update_order_success'
    UPDATE_ORDER_REJECTED = 'update_order_rejected'
    TRANSFER_TO_SELLER_BALANCE = 'transfer_to_seller_balance'
    REFUND_BUYER = 'refund_buyer'

    _queue_mapping = {
        UPDATE_PRODUCT_QUOTA: 'product',
        APPROVE_ORDER_PENDING: 'order',
        RESERVE_BUYER_CREDIT: 'user',
        TRANSFER_TO_SELLER_BALANCE: 'user',
        REFUND_BUYER: 'user'
    }

    @classmethod
    def get_queue(cls, name):
        return cls._queue_mapping[name]
