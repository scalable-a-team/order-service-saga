import os

main = 'tasks'
broker_url = os.getenv('CELERY_BROKER_URL', default='amqp://guest:guest@localhost:5672//')
result_backend = os.getenv('CELERY_RESULT_BACKEND')
task_acks_late = True
task_acks_on_failure_or_timeout = False
task_reject_on_worker_lost = True

task_routes = {
    'tasks.create_order': {'queue': 'order'},
    'tasks.revert_create_order': {'queue': 'order'},
    'tasks.update_order_success': {'queue': 'order'},
    'tasks.update_order_rejected': {'queue': 'order'}
}
