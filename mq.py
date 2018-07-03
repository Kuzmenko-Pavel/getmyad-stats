# -*- coding: utf-8 -*-
from amqplib import client_0_8 as amqp


class MQ():
    def __init__(self):
        self.conn = amqp.Connection(host='amqp.yottos.com',
                               userid='worker',
                               password='worker',
                               virtual_host='worker',
                               insist=True)
        self.ch = self.conn.channel()
        self.ch.exchange_declare(exchange="getmyad", type="topic", durable=True, auto_delete=False, passive=False)

    def __del__(self):
        self.ch.close()

    def rating_informer_update(self, guid_int):
        msg = amqp.Message(str(guid_int))
        self.ch.basic_publish(msg, exchange='getmyad', routing_key='informer.updateRating')

    def rating_informer_delete(self, guid_int):
        msg = amqp.Message(str(guid_int))
        self.ch.basic_publish(msg, exchange='getmyad', routing_key='informer.updateRating')

    def offer_update(self, offer_Id, campaign_id):
        msg = 'Offer:%s,Campaign:%s' % (offer_Id, campaign_id)
        msg = amqp.Message(msg)
        self.ch.basic_publish(msg, exchange='getmyad', routing_key='advertise.update')

    def offer_delete(self, offer_Id, campaign_id):
        msg = 'Offer:%s,Campaign:%s' % (offer_Id, campaign_id)
        msg = amqp.Message(msg)
        self.ch.basic_publish(msg, exchange='getmyad', routing_key='advertise.delete')

    def informer_rating_update(self):
        msg = amqp.Message('')
        self.ch.basic_publish(msg, exchange='getmyad', routing_key='rating.informer')

    def campaign_rating_update(self):
        msg = amqp.Message('')
        self.ch.basic_publish(msg, exchange='getmyad', routing_key='rating.campaign')

    def offer_rating_update(self):
        msg = amqp.Message('')
        self.ch.basic_publish(msg, exchange='getmyad', routing_key='rating.offer')
