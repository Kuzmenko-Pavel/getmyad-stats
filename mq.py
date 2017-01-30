# encoding: utf-8
from amqplib import client_0_8 as amqp


class MQ():
    def _get_channel(self):
        conn = amqp.Connection(host='rg.yottos.com',
                               userid='old_worker',
                               password='old_worker',
                               virtual_host="/",
                               insist=True)
        ch = conn.channel()
        return ch

    def _get_worker_channel(self):
        ''' Подключается к брокеру mq '''
        conn = amqp.Connection(host='rg.yottos.com',
                               userid='worker',
                               password='worker',
                               virtual_host='worker',
                               insist=True)
        ch = conn.channel()
        ch.exchange_declare(exchange="getmyad", type="topic", durable=False, auto_delete=True)
        return ch

    def rating_informer_update(self, guid_int):
        ch = self._get_channel()
        ch_worker = self._get_worker_channel()
        msg = amqp.Message(str(guid_int))
        ch.basic_publish(msg, exchange='getmyad', routing_key='informer.updateRating')
        ch.close()
        ch_worker.basic_publish(msg, exchange='getmyad', routing_key='informer.updateRating')
        ch_worker.close()

    def rating_informer_delete(self, guid_int):
        ch = self._get_channel()
        ch_worker = self._get_worker_channel()
        msg = amqp.Message(str(guid_int))
        ch.basic_publish(msg, exchange='getmyad', routing_key='informer.updateRating')
        ch.close()
        ch_worker.basic_publish(msg, exchange='getmyad', routing_key='informer.updateRating')
        ch_worker.close()

    def offer_update(self, offer_Id, campaign_id):
        ch = self._get_channel()
        ch_worker = self._get_worker_channel()
        msg = 'Offer:%s,Campaign:%s' % (offer_Id, campaign_id)
        msg = amqp.Message(msg)
        ch.basic_publish(msg, exchange='getmyad', routing_key='advertise.update')
        ch.close()
        ch_worker.basic_publish(msg, exchange='getmyad', routing_key='advertise.update')
        ch_worker.close()

    def offer_delete(self, offer_Id, campaign_id):
        ch = self._get_channel()
        ch_worker = self._get_worker_channel()
        msg = 'Offer:%s,Campaign:%s' % (offer_Id, campaign_id)
        msg = amqp.Message(msg)
        ch.basic_publish(msg, exchange='getmyad', routing_key='advertise.delete')
        ch.close()
        ch_worker.basic_publish(msg, exchange='getmyad', routing_key='advertise.delete')
        ch_worker.close()
