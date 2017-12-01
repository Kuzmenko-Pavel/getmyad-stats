# encoding: utf-8
import datetime


class GetmyadClean():
    def __init__(self, db):
        self.db = db

    def clean_ip_blacklist(self):
        u"""Удаляет старые записи из чёрного списка"""
        print(self.db.blacklist.ip.remove({'dt': {'$lte': datetime.datetime.now() - datetime.timedelta(weeks=2)}}))

    def decline_unconfirmed_moneyout_requests(self):
        u"""Отклоняет заявки, которые пользователи не подтвердили в течении трёх
            дней"""
        clean_to_date = datetime.datetime.now() - datetime.timedelta(days=3)
        print('Decline unconfirmed %s' % clean_to_date)
        self.db.money_out_request.remove(
            {'user_confirmed': {'$ne': True},
             'approved': {'$ne': True},
             'date': {'$lte': clean_to_date}})

    def delete_old_stats(self):
        u"""Удаляем старую статистику"""
        delete_to_date = datetime.datetime.now() - datetime.timedelta(days=3)
        print(self.db.stats.daily.remove({'date': {'$lt': delete_to_date}}))
        print(self.db.stats.daily.raw.remove({'date': {'$lt': delete_to_date}}))

    def delete_click_rejected(self):
        u"""Удаляем старую статистику по отклонённым кликам"""
        delete_to_date = datetime.datetime.now() - datetime.timedelta(days=4)
        print(self.db.clicks.rejected.remove({'dt': {'$lt': delete_to_date}}))

    def delete_old_offers(self, rpc):
        CampaignId = self.db.campaign.group(key={'guid': True}, condition={}, reduce='function(obj,prev){}', initial={})
        CampaignId = map(lambda x: x['guid'], CampaignId)
        count = 0
        for x in self.db.offer.find():
            if x['campaignId'] not in CampaignId:
                print("Delete offer guid : %s" % x)
                self.db.offer.remove(x)
                count += 1
        print("Deleted %s offers" % count)
