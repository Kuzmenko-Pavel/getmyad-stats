# -*- coding: utf-8 -*-
import datetime
from collections import defaultdict

import bson.objectid
import pymongo
from pymongo.errors import BulkWriteError

from mq import MQ


class GetmyadRating(object):
    def __init__(self, db, pool):
        self.db = db
        self.pool = pool
        self.mq = MQ()

    def importWorkerData(self):
        u"""Mongo worker data import"""
        elapsed_start_time = datetime.datetime.now()
        rating_buffer = defaultdict(int)
        campaign_rating_buffer = defaultdict(int)
        filds = {'_id': True, 'id': True, 'inf': True, 'campaignId': True,
                 'id_int': True, 'inf_int': True, 'campaignId_int': True, 'retargeting': True,
                 'request': True, 'active': True}
        print("Start import rating Data")
        for db2 in self.pool:
            try:
                print(db2)

                try:
                    last_processed_id = db2.config.find_one({'key': 'impressions rating last _id'})['value']
                except Exception as e:
                    last_processed_id = None
                if not isinstance(last_processed_id, bson.objectid.ObjectId):
                    last_processed_id = None

                try:
                    cursor = db2['log.impressions'].find({}, filds).sort("$natural", pymongo.DESCENDING)
                except Exception as e:
                    print("Cursor ERROR", e)
                    continue
                try:
                    end_id = cursor[0]['_id']  # Последний id, который будет обработан в этот раз
                    print(end_id)
                except:
                    print("importImpressionsFromMongo: nothing to do")
                    continue

                try:
                    for x in cursor:
                        try:
                            if last_processed_id is not None and x['_id'] == last_processed_id:
                                break
                            request = x.get('request', 'initial')
                            active = x.get('active', 'initial')
                            key_rating = (
                                x['id'], x['inf'].lower(), x['campaignId'].lower(),
                                int(x['id_int']), int(x['inf_int']), int(x['campaignId_int'])
                            )
                            campaign_key_rating = (
                                x['inf'].lower(), x['campaignId'].lower(), int(x['inf_int']), int(x['campaignId_int'])
                            )
                            if active == 'initial' and request == 'initial':
                                campaign_rating_buffer[campaign_key_rating] += 1
                                if not x.get('retargeting', False):
                                    rating_buffer[key_rating] += 1
                        except Exception as e:
                            print("Iteration error", e)
                            pass
                except Exception as e:
                    print("For cursor error", e)
                    pass
                print("read base complite")
                db2.config.update({'key': 'impressions rating last _id'}, {'$set': {'value': end_id}}, True)
            except Exception as e:
                print("Worker base error", e)
                pass

        self.db.reset_error_history()

        operations_offer = []
        operations_stats_daily = []
        operations_campaign = []

        for key, value in rating_buffer.iteritems():
            try:
                operations_offer.append(
                    pymongo.UpdateOne({'guid': key[0]},
                                      {'$inc': {'impressions': value, 'full_impressions': value}}, upsert=False)
                )
            except Exception as ex:
                print(ex, "worker_stats", key, value)
            try:
                operations_stats_daily.append(
                    pymongo.UpdateOne({'adv_int': int(key[4]),
                                       'guid_int': int(key[3]),
                                       'campaignId_int': int(key[5])
                                       },
                                      {
                                          '$inc': {'impressions': value, 'full_impressions': value},
                                          '$set': {'adv': key[1], 'guid': key[0], 'campaignId': key[2]}
                                      },
                                      upsert=True)
                )
            except Exception as ex:
                print(ex, "worker_stats", key, value)

        # for key, value in campaign_rating_buffer.iteritems():
        #     try:
        #         operations_campaign.append(
        #             pymongo.UpdateOne(
        #                 {'adv': key[0], 'adv_int': key[2], 'campaignId': key[1], 'campaignId_int': key[3]},
        #                 {'$inc': {'impressions': value, 'full_impressions': value}}, upsert=True)
        #         )
        #     except Exception as ex:
        #         print(ex, "worker_stats", key, value)

        try:
            self.db.offer.bulk_write(operations_offer, ordered=False)
        except BulkWriteError as bwe:
            print(bwe.details)

        try:
            self.db.stats_daily.rating.bulk_write(operations_stats_daily, ordered=False)
        except BulkWriteError as bwe:
            print(bwe.details)

        # try:
        #     self.db.campaign.rating.bulk_write(operations_campaign, ordered=False)
        # except BulkWriteError as bwe:
        #     print(bwe.details)

        elapsed = (datetime.datetime.now() - elapsed_start_time).seconds
        print("Stop import rating Data in %s second" % elapsed)
        if self.db.previous_error():
            print("Database error", self.db.previous_error())

    def importClicksFromMongo(self):
        u"""Обработка кликов из mongo"""
        elapsed_start_time = datetime.datetime.now()
        # _id последней записи, обработанной скриптом. Если не было обработано ничего, равно None 

        try:
            last_processed_id = self.db.config.find_one({'key': 'clicks rating last _id'})['value']
        except Exception as e:
            last_processed_id = None
        if not isinstance(last_processed_id, bson.objectid.ObjectId):
            last_processed_id = None

        cursor = self.db['clicks'].find().sort("$natural", pymongo.DESCENDING)
        try:
            end_id = cursor[0]['_id']  # Последний id, который будет обработан в этот раз
        except:
            print("importClicksFromMongo: nothing to do")
            return

        processed_records = 0
        operations_offer = []
        operations_stats_daily = []
        operations_campaign = []

        for x in cursor:
            if last_processed_id is not None and x['_id'] == last_processed_id:
                break
            processed_records += 1
            if x['unique']:
                value = {'$inc': {'clicks': 1, 'full_clicks': 1}}
                try:
                    operations_stats_daily.append(
                        pymongo.UpdateOne({'adv': x['inf'],
                                           'guid': x['offer'],
                                           'campaignId': x['campaignId']},
                                          value,
                                          upsert=False)
                    )
                except Exception as ex:
                    print(ex, "worker_stats", x)
                # try:
                #     operations_campaign.append(
                #         pymongo.UpdateOne(
                #             {'adv': x['inf'], 'campaignId': x['campaignId']},
                #             value, upsert=False)
                #     )
                # except Exception as ex:
                #     print(ex, "worker_stats", x)

                try:
                    value['$max'] = {'cost': float(x.get('adload_cost', 0.0))}
                    operations_offer.append(
                        pymongo.UpdateOne({'guid': x['offer']},
                                          value, upsert=False)
                    )
                except Exception as ex:
                    print(ex, "worker_stats", x)

        self.db.config.update({'key': 'clicks rating last _id'}, {'$set': {'value': end_id}}, True)

        try:
            self.db.offer.bulk_write(operations_offer, ordered=False)
        except BulkWriteError as bwe:
            print(bwe.details)

        try:
            self.db.stats_daily.rating.bulk_write(operations_stats_daily, ordered=False)
        except BulkWriteError as bwe:
            print(bwe.details)

        # try:
        #     self.db.campaign.rating.bulk_write(operations_campaign, ordered=False)
        # except BulkWriteError as bwe:
        #     print(bwe.details)

        print("Finished %s records in %s seconds" % (
            processed_records, (datetime.datetime.now() - elapsed_start_time).seconds))

    def createOfferRating(self):
        msg = {}
        campaignIdList = [x['guid'] for x in
                          self.db.campaign.find({"showConditions.retargeting": False, "status": "working"}, {
                              'guid': 1,
                              '_id': -1
                          })]
        queri = {"campaignId": {"$in": campaignIdList}}
        fields = {'_id': 0, 'impressions': 1, 'clicks': 1, 'full_impressions': 1, 'full_clicks': 1, 'cost': 1,
                  'guid': 1, 'campaignId': 1}
        offers = self.db.offer.find(queri, fields)
        offer_count = 0
        date_update = datetime.datetime.now()
        operations = []
        for offer in offers:
            udata = {}
            impressions = offer.get('impressions', 0)
            clicks = offer.get('clicks', 0)
            full_impressions = offer.get('full_impressions', 0)
            full_clicks = offer.get('full_clicks', 0)
            offer_cost = offer.get('cost', 0.1)
            if clicks and impressions > 0:
                ctr = ((float(clicks) / impressions) * 100)
            else:
                ctr = 0

            if (full_clicks and full_impressions) > 0:
                full_ctr = ((float(full_clicks) / full_impressions) * 100)
            else:
                full_ctr = 0
            if impressions > 1500:
                rating = (ctr * offer_cost) * 100000
                udata['rating'] = round(rating, 4)
                udata['last_rating_update'] = date_update
                udata['old_impressions'] = impressions
                udata['old_clicks'] = clicks
            if full_impressions > 1500:
                rating = (full_ctr * offer_cost) * 100000
                udata['full_rating'] = round(rating, 4)
                udata['last_full_rating_update'] = date_update
                offer_count += 1
                msg[offer['guid']] = offer['campaignId']
            if len(udata) > 0:
                try:
                    operations.append(
                        pymongo.UpdateOne({'guid': offer['guid']}, {'$set': udata}, upsert=False)
                    )
                except Exception as ex:
                    print(ex, "buffer", offer['guid'], udata)

        try:
            self.db.offer.bulk_write(operations, ordered=False)
        except BulkWriteError as bwe:
            print(bwe.details)

        for key, value in msg.iteritems():
            self.mq.offer_update(key, value)
        self.mq.offer_rating_update()
        print("Created %d rating for offer" % offer_count)

    def createCampaignRatingForInformers(self):
        # date_update = datetime.datetime.now()
        # costs = defaultdict(list)
        # for item in self.db.offer.find({}, {'campaignId': True, 'cost': True}):
        #     costs[item['campaignId']].append(item['cost'])
        # campaigns = self.db.campaign.rating.find()
        # for campaign in campaigns:
        #     udata = {}
        #     impressions = campaign.get('impressions', 0)
        #     clicks = campaign.get('clicks', 0)
        #     full_impressions = campaign.get('full_impressions', 0)
        #     full_clicks = campaign.get('full_clicks', 0)
        #     cost = costs.get(campaign['campaignId'], [0.5])
        #     if len(cost) > 0:
        #         cost = sum(cost) / len(cost)
        #     else:
        #         cost = 0.5
        #     if (clicks and impressions) > 0:
        #         ctr = ((float(clicks) / impressions) * 100)
        #     else:
        #         ctr = 0
        #
        #     if (full_clicks and full_impressions) > 0:
        #         full_ctr = ((float(full_clicks) / full_impressions) * 100)
        #     else:
        #         full_ctr = 0
        #     if (impressions > 1500):
        #         rating = (ctr * cost) * 100000
        #         udata['rating'] = round(rating, 4)
        #         udata['last_rating_update'] = date_update
        #         udata['old_impressions'] = impressions
        #         udata['cost'] = cost
        #         udata['old_clicks'] = clicks
        #
        #     if (full_impressions > 1500):
        #         rating = (full_ctr * cost) * 100000
        #         udata['full_rating'] = round(rating, 4)
        #         udata['cost'] = cost
        #         udata['last_full_rating_update'] = date_update
        #
        #     if len(udata) > 0:
        #         self.db.campaign.rating.update({'campaignId': campaign['campaignId'],
        #                                         'campaignId_int': campaign['campaignId_int'],
        #                                         'adv': campaign['adv'],
        #                                         'adv_int': campaign['adv_int']},
        #                                        {'$set': udata}, upsert=False)
        self.mq.campaign_rating_update()

    def createOfferRatingForInformers(self):
        date_update = datetime.datetime.now()
        offer_count = 0
        costs = {}
        msg = {}
        informersBySite = {}
        informersByTitle = {}
        for informer in self.db.informer.find({}, {'guid': True, 'domain': True, 'title': True}):
            try:
                informersBySite[informer['guid']] = informer.get('domain', 'NOT DOMAIN')
                informersByTitle[informer['guid']] = informer.get('title', 'NOT TITLE')
            except:
                pass
        campaignIdList = []
        campaign = {}
        for x in self.db.campaign.find({"showConditions.retargeting": False, "status": "working"},
                                       {'guid': 1, 'title': 1, '_id': -1}):
            campaignIdList.append(x['guid'])
            campaign[x['guid']] = x['title']

        queri = {"campaignId": {"$in": campaignIdList}}
        for item in self.db.offer.find(queri, {'guid': 1, 'cost': 1, 'title': 1, '_id': -1}):
            costs[item['guid']] = [item['cost'], item['title']]

        offers = self.db.stats_daily.rating.find(queri)
        operations = []
        for offer in offers:
            udata = {}
            impressions = offer.get('impressions', 0)
            clicks = offer.get('clicks', 0)
            full_impressions = offer.get('full_impressions', 0)
            full_clicks = offer.get('full_clicks', 0)
            offer_cost, offer_title = costs.get(offer['guid'], [0.5, ''])
            campaignTitle = campaign.get(offer['campaignId'], '')
            if (clicks and impressions) > 0:
                ctr = ((float(clicks) / impressions) * 100)
            else:
                ctr = 0

            if (full_clicks and full_impressions) > 0:
                full_ctr = ((float(full_clicks) / full_impressions) * 100)
            else:
                full_ctr = 0
            if impressions > 1500:
                rating = (ctr * offer_cost) * 100000
                udata['rating'] = round(rating, 4)
                udata['last_rating_update'] = date_update
                udata['old_impressions'] = impressions
                udata['cost'] = offer_cost
                udata['adv_domain'] = informersBySite.get(offer['adv'], '')
                udata['adv_title'] = informersByTitle.get(offer['adv'], '')
                udata['old_clicks'] = clicks
                udata['title'] = offer_title
                udata['campaignTitle'] = campaignTitle
            else:
                if offer.get('last_rating_update') is None:
                    udata['adv_domain'] = informersBySite.get(offer['adv'], '')
                    udata['adv_title'] = informersByTitle.get(offer['adv'], '')
                    udata['title'] = offer_title
                    udata['campaignTitle'] = campaignTitle

            if full_impressions > 1500:
                offer_count += 1
                rating = (full_ctr * offer_cost) * 100000
                msg[offer['adv']] = offer['adv_int']
                udata['full_rating'] = round(rating, 4)
                udata['cost'] = offer_cost
                udata['last_full_rating_update'] = date_update
                udata['title'] = offer_title
                udata['campaignTitle'] = campaignTitle
            if len(udata) > 0:
                try:
                    operations.append(
                        pymongo.UpdateOne({'guid_int': offer['guid_int'],
                                           'campaignId_int': offer['campaignId_int'],
                                           'adv_int': offer['adv_int']},
                                          {'$set': udata}, upsert=False)
                    )
                except Exception as ex:
                    print(ex, "buffer", offer['guid'], udata)

        try:
            self.db.stats_daily.rating.bulk_write(operations, ordered=False)
        except BulkWriteError as bwe:
            print(bwe.details)

        for key, value in msg.iteritems():
            self.mq.rating_informer_update(value)
        self.mq.informer_rating_update()
        print("Created %d rating for offer-informer" % offer_count)

    def delete_old_rating_stats(self):
        u"""Удаляем старую статистику"""
        offersId = []
        campaignIdList = [x['guid'] for x in self.db.campaign.find({"showConditions.retargeting": False}, {'guid': 1})]
        informerIdList = [x['guid'] for x in self.db.informer.find({}, {'guid': 1})]
        print(informerIdList)
        queri = {"campaignId": {"$in": campaignIdList}}
        for item in self.db.offer.find(queri, {"guid": 1, "_id": 0}):
            offersId.append(item['guid'])

        a = self.db.stats_daily.rating.delete_many({'adv': {'$nin': informerIdList}})
        d = self.db.stats_daily.rating.delete_many({'full_rating': 0})
        i = self.db.stats_daily.rating.delete_many({'guid': {'$nin': offersId}})
        # Сбрасуем показы и клики в товарах
        y = self.db.offer.update({'$or': [{'impressions': {'$lte': 0}}, {'impressions': {'$gte': 1500}},
                                          {'impressions': {'$exists': False}}]},
                                 {'$set': {'impressions': 0, 'clicks': 0}}, multi=True, w=1)
        # Сбрасуем показы и клики в товарах по рекламному блоку
        z = self.db.stats_daily.rating.update({'$or': [{'impressions': {'$lte': 0}}, {'impressions': {'$gte': 1500}},
                                                       {'impressions': {'$exists': False}}]},
                                              {'$set': {'impressions': 0, 'clicks': 0}}, multi=True, w=1)

        print("records deleted %s" % i.deleted_count)
        print("records offer/inf 0 rating deleted %s" % d.deleted_count)
        print("records inf rating deleted %s" % a.deleted_count)
        print("clearn offer imp/click %s" % y)
        print("clearn offer/inf imp/click %s" % z)

    def trunkete_rating_stats(self):
        u"""Обризаем показы"""
        of_im = 1000000
        of_inf_im = 250000
        for item in self.db.offer.find({'full_impressions': {'$gte': of_im * 4}, 'retargeting': False}):
            full_impressions = item.get('full_impressions', 0)
            full_clicks = item.get('full_clicks', 0)

            if (full_clicks and full_impressions) > 0:
                propor = (float(full_clicks) / full_impressions)
            else:
                item['full_impressions'] = of_im * 3
                self.db.offer.save(item)
                continue

            item['full_impressions'] = of_im * 3
            item['full_clicks'] = int(propor * of_im * 3)
            self.db.offer.save(item)

        for item in self.db.stats_daily.rating.find({'full_impressions': {'$gte': of_inf_im * 4}}):
            full_impressions = item.get('full_impressions', 0)
            full_clicks = item.get('full_clicks', 0)

            if (full_clicks and full_impressions) > 0:
                propor = (float(full_clicks) / full_impressions)
            else:
                item['full_impressions'] = of_inf_im * 4
                self.db.stats_daily.rating.save(item)
                continue

            item['full_impressions'] = of_inf_im * 4
            item['full_clicks'] = int(propor * of_inf_im * 4)
            self.db.stats_daily.rating.save(item)
