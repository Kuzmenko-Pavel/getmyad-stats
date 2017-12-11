# -*- coding: UTF-8 -*-
import datetime


class GetmyadClean():
    def __init__(self, db):
        self.db = db

    def clean_ip_blacklist(self):
        """Удаляет старые записи из чёрного списка"""
        print(self.db.blacklist.ip.remove({'dt': {'$lte': datetime.datetime.now() - datetime.timedelta(weeks=2)}}))

    def decline_unconfirmed_moneyout_requests(self):
        """Отклоняет заявки, которые пользователи не подтвердили в течении трёх
            дней"""
        clean_to_date = datetime.datetime.now() - datetime.timedelta(days=3)
        print('Decline unconfirmed %s' % clean_to_date)
        self.db.money_out_request.remove(
            {'user_confirmed': {'$ne': True},
             'approved': {'$ne': True},
             'date': {'$lte': clean_to_date}})

    def delete_old_stats(self):
        """Удаляем старую статистику"""
        delete_to_date = datetime.datetime.now() - datetime.timedelta(days=3)
        print(self.db.stats.daily.remove({'date': {'$lt': delete_to_date}}))
        print(self.db.stats.daily.raw.remove({'date': {'$lt': delete_to_date}}))

    def delete_click_rejected(self):
        """Удаляем старую статистику по отклонённым кликам"""
        delete_to_date = datetime.datetime.now() - datetime.timedelta(days=4)
        print(self.db.clicks.rejected.remove({'dt': {'$lt': delete_to_date}}))

    def delete_old_offers(self):
        campaign_id = self.db.campaign.group(key={'guid': True},
                                             condition={},
                                             reduce='function(obj,prev){}',
                                             initial={})
        campaign_id = map(lambda x: x['guid'], campaign_id)
        count = 0
        for x in self.db.offer.find():
            if x['campaignId'] not in campaign_id:
                print("Delete offer guid : %s" % x)
                self.db.offer.remove(x)
                count += 1
        print("Deleted %s offers" % count)
