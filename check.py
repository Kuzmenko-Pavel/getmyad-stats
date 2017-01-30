# encoding: utf-8
import datetime
import pymongo
import xmlrpclib
from adload_data import AdloadData, mssql_connection_adload


class GetmyadCheck():
    def check_outdated_campaigns(self, db, rpc):
        ''' Иногда AdLoad не оповещает GetMyAd об остановке кампании, об отработке
            парсера и т.д. Это приводит к тому, что кампания продолжает крутиться
            в GetMyAd, но клики не засчитываются и записываются в clicks.error.
            Данная задача проверяет, не произошло ли за последнее время несколько
            таких ошибок и, если произошло, обновляет кампанию. '''

        WATCH_LAST_N_MINUTES = 60  # Смотрим лог за последние N минут
        ALLOWED_ERRORS = 1  # Допустимое количество ошибок на одну кампанию

        # Смотрим лог ошибок, начиная с конца
        c = db['clicks.error'].find().sort('$natural', pymongo.DESCENDING)
        now = datetime.datetime.now()
        campaigns = []
        for item in c:
            if (now - item['dt']).seconds > WATCH_LAST_N_MINUTES * 60:
                break
            guid = item.get('campaignId', '')
            if guid is not None and len(guid) > 1:
                campaigns.append(guid)
        campaigns = set(campaigns)
        campaigns = list(campaigns)
        for ca in db.campaign.find({"status": "working", "guid": {'$in': campaigns}}):
            result = rpc.campaign_update(ca['guid'])
            print u"Обнавляю компанию %s %s" % (ca['guid'], result)

    def check_campaigns(self, db, rpc):
        ad = AdloadData(mssql_connection_adload())
        c = db['campaign'].find().sort('$natural', pymongo.DESCENDING)

        for item in c:
            status = ad.campaign_details(item['guid'])
            if not status:
                result_stop = rpc.campaign_stop(item['guid'])
                print u"Кампания не запущена в AdLoad или запрещена для показа в GetMyAd. \n" + \
                      u"Останавливаю кампанию: %s %s %s" % (item['guid'], item['title'], result_stop)
                continue

            status = ad.campaign_check(item['guid'])
            if not status:
                result_stop = rpc.campaign_hold(item['guid'])
                print u"В кампании нет активных предложений. \n" + \
                      u"Возможные причины: на счету кампании нет денег, не отработал парсер Рынка (для интернет-магазинов).\n" + \
                      u"Замораживаю кампанию: %s %s %s" % (item['guid'], item['title'], result_stop)
