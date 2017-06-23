# encoding: utf-8
import datetime


class GetmyadClean():
    def clean_ip_blacklist(self, db):
        u"""Удаляет старые записи из чёрного списка"""
        print db.blacklist.ip.remove({'dt': {'$lte': datetime.datetime.now() - datetime.timedelta(weeks=2)}})

    def decline_unconfirmed_moneyout_requests(self, db):
        u"""Отклоняет заявки, которые пользователи не подтвердили в течении трёх
            дней"""
        clean_to_date = datetime.datetime.now() - datetime.timedelta(days=3)
        print 'Decline unconfirmed %s' % clean_to_date
        db.money_out_request.remove(
            {'user_confirmed': {'$ne': True},
             'approved': {'$ne': True},
             'date': {'$lte': clean_to_date}})

    def delete_old_stats(self, db):
        u"""Удаляем старую статистику"""
        delete_to_date = datetime.datetime.now() - datetime.timedelta(days=3)
        y = db.stats.daily.remove({'date': {'$lt': delete_to_date}})
        y = db.stats.daily.raw.remove({'date': {'$lt': delete_to_date}})
        print y

    def delete_click_rejected(self, db):
        u"""Удаляем старую статистику по отклонённым кликам"""
        delete_to_date = datetime.datetime.now() - datetime.timedelta(days=4)
        print db.clicks.rejected.remove({'dt': {'$lt': delete_to_date}})

    def delete_old_offers(self, db, rpc):
        CampaignId = db.campaign.group(key={'guid': True}, condition={}, reduce='function(obj,prev){}', initial={})
        CampaignId = map(lambda x: x['guid'], CampaignId)
        count = 0
        for x in db.offer.find():
            if x['campaignId'] not in CampaignId:
                print "Delete offer guid : ", x
                db.offer.remove(x)
                count += 1
        print "Deleted %s offers" % count
