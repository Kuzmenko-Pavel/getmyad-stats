# encoding: utf-8
import datetime

import pymongo
import requests

from adload_data import AdloadData, mssql_connection_adload


class GetmyadCheck():
    def __init__(self, db, rpc):
        self.db = db
        self.rpc = rpc

    def check_outdated_campaigns(self):
        ''' Иногда AdLoad не оповещает GetMyAd об остановке кампании, об отработке
            парсера и т.д. Это приводит к тому, что кампания продолжает крутиться
            в GetMyAd, но клики не засчитываются и записываются в clicks.error.
            Данная задача проверяет, не произошло ли за последнее время несколько
            таких ошибок и, если произошло, обновляет кампанию. '''

        WATCH_LAST_N_MINUTES = 60  # Смотрим лог за последние N минут
        ALLOWED_ERRORS = 1  # Допустимое количество ошибок на одну кампанию

        # Смотрим лог ошибок, начиная с конца
        c = self.db['clicks.error'].find().sort('$natural', pymongo.DESCENDING)
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
        for ca in self.db.campaign.find({"status": "working", "guid": {'$in': campaigns}}):
            result = self.rpc.campaign_update(ca['guid'])
            print("Обнавляю компанию %s %s" % (ca['guid'], result))

    def check_campaigns(self):
        ad = AdloadData(mssql_connection_adload())
        c = self.db['campaign'].find().sort('$natural', pymongo.DESCENDING)

        for item in c:
            status = ad.campaign_details(item['guid'])
            if not status:
                result_stop = self.rpc.campaign_stop(item['guid'])
                print("Кампания не запущена в AdLoad или запрещена для показа в GetMyAd. \n"
                      "Останавливаю кампанию: %s %s %s" % (item['guid'], item['title'], result_stop))
                continue

            status = ad.campaign_check(item['guid'])
            if not status:
                result_stop = self.rpc.campaign_hold(item['guid'])
                print("В кампании нет активных предложений. \n "
                      "Возможные причины: на счету кампании нет денег,"
                      "не отработал парсер Рынка (для интернет-магазинов).\n "
                      "Замораживаю кампанию: %s %s %s" % (item['guid'], item['title'], result_stop))

    def check_cdn(self):
        date = datetime.datetime.now() - datetime.timedelta(minutes=15)
        link = []
        headers = {'X-Cache-Update': '1'}
        cdns = ['cdn.srv-10.yottos.com', 'cdn.srv-11.yottos.com', 'cdn.srv-12.yottos.com']
        adv = self.db['informer'].find({'lastModified': {'$gte': date}})

        for item in adv:
            guid = item['guid']
            link.append('/block/%s.json' % guid)
            link.append('/block/%s.js' % guid)
            link.append('/getmyad/%s.js' % guid)

        for item in link:
            for cdn in cdns:
                url = 'http://%s%s' % (cdn, item)
                r = requests.get(url, headers=headers, verify=False)
                print('%s - %s' % (url, r.status_code))
