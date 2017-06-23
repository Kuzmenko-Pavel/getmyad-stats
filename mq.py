# encoding: utf-8
from amqplib import client_0_8 as amqp


class MQ():
    def _get_worker_channel(self):
        ''' Подключается к брокеру mq '''
        conn = amqp.Connection(host='srv-4.yottos.com',
                               userid='worker',
                               password='worker',
                               virtual_host='worker',
                               insist=True)
        ch = conn.channel()
        ch.exchange_declare(exchange="getmyad", type="topic", durable=False, auto_delete=True)
        return ch

    def rating_informer_update(self, guid_int):
        ch_worker = self._get_worker_channel()
        msg = amqp.Message(str(guid_int))
        ch_worker.basic_publish(msg, exchange='getmyad', routing_key='informer.updateRating')
        ch_worker.close()

    def rating_informer_delete(self, guid_int):
        ch_worker = self._get_worker_channel()
        msg = amqp.Message(str(guid_int))
        ch_worker.basic_publish(msg, exchange='getmyad', routing_key='informer.updateRating')
        ch_worker.close()

    def offer_update(self, offer_Id, campaign_id):
        ch_worker = self._get_worker_channel()
        msg = 'Offer:%s,Campaign:%s' % (offer_Id, campaign_id)
        msg = amqp.Message(msg)
        ch_worker.basic_publish(msg, exchange='getmyad', routing_key='advertise.update')
        ch_worker.close()

    def offer_delete(self, offer_Id, campaign_id):
        ch_worker = self._get_worker_channel()
        msg = 'Offer:%s,Campaign:%s' % (offer_Id, campaign_id)
        msg = amqp.Message(msg)
        ch_worker.basic_publish(msg, exchange='getmyad', routing_key='advertise.delete')
        ch_worker.close()

    def informer_rating_update(self):
        ch_worker = self._get_worker_channel()
        msg = amqp.Message('')
        ch_worker.basic_publish(msg, exchange='getmyad', routing_key='rating.informer')
        ch_worker.close()

    def campaign_rating_update(self):
        ch_worker = self._get_worker_channel()
        msg = amqp.Message('')
        ch_worker.basic_publish(msg, exchange='getmyad', routing_key='rating.campaign')
        ch_worker.close()

    def offer_rating_update(self):
        ch_worker = self._get_worker_channel()
        msg = amqp.Message('')
        ch_worker.basic_publish(msg, exchange='getmyad', routing_key='rating.offer')
        ch_worker.close()