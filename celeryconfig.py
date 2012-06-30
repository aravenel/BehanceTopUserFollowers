#BROKER_URL = 'sqla+sqlite:///celerydb.sqlite'
#BROKER_URL = 'amqp://ravenel:eleven12@localhost:5672/myvhost'
BROKER_HOST = 'localhost'
BROKER_PORT = 5672
BROKER_USER = 'ravenel'
BROKER_PASSWORD = 'eleven12'
BROKER_VHOST = 'myvhost'

CELERY_IMPORTS = ('tasks',)
