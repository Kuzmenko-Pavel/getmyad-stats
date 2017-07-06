# -*- coding: utf-8 -*-
from collections import defaultdict

import bson.objectid
import datetime
import pymongo

from mq import MQ


class GetmyadRating(object):
    def importWorkerData(self, db, pool):
        u"""Mongo worker data import"""
        elapsed_start_time = datetime.datetime.now()
        rating_buffer = defaultdict(int)
        campaign_rating_buffer = defaultdict(int)
        filds = {'_id': True, 'id': True, 'inf': True, 'campaignId': True,
                 'id_int': True, 'inf_int': True, 'campaignId_int': True, 'retargeting': True}
        print "Start import rating Data"
        for db2 in pool:
            try:
                print db2
                last_processed_id = None
                try:
                    last_processed_id = db2.config.find_one({'key': 'impressions rating last _id'})['value']
                except:
                    last_processed_id = None
                if not isinstance(last_processed_id, bson.objectid.ObjectId):
                    last_processed_id = None

                try:
                    cursor = db2['log.impressions'].find({}, filds).sort("$natural", pymongo.DESCENDING)
                except Exception as e:
                    print "Cursor ERROR"
                    print e
                    return
                try:
                    end_id = cursor[0]['_id']  # Последний id, который будет обработан в этот раз
                    print end_id
                except:
                    print "importImpressionsFromMongo: nothing to do"
                    return

                try:
                    for x in cursor:
                        try:
                            if last_processed_id <> None and x['_id'] == last_processed_id:
                                break

                            key_rating = (
                                x['id'], x['inf'].lower(), x['campaignId'].lower(),
                                x['id_int'], x['inf_int'], x['campaignId_int']
                            )
                            campaign_key_rating = (
                                x['inf'].lower(), x['campaignId'].lower(), x['inf_int'], x['campaignId_int']
                            )
                            campaign_rating_buffer[campaign_key_rating] += 1
                            if not x.get('retargeting', False):
                                rating_buffer[key_rating] += 1
                        except Exception as e:
                            print "Iteration error"
                            print e
                            pass
                except Exception as e:
                    print "For cursor error"
                    print e
                    pass
                print "read base complite"
                db2.config.update({'key': 'impressions rating last _id'}, {'$set': {'value': end_id}}, True)
            except Exception as e:
                print "Worker base error"
                print e
                pass

        db.reset_error_history()
        for key, value in rating_buffer.iteritems():
            try:
                db.offer.update({'guid': key[0]},
                                {'$inc': {'impressions': value, 'full_impressions': value}}, False)
                db.stats_daily.rating.update({'adv': key[1],
                                              'adv_int': key[4],
                                              'guid': key[0],
                                              'guid_int': key[3],
                                              'campaignId': key[2],
                                              'campaignId_int': key[5]
                                              },
                                             {'$inc': {'impressions': value, 'full_impressions': value}},
                                             upsert=True)
            except Exception, ex:
                print ex
                print "rating_buffer"
                print key, value

        for key, value in campaign_rating_buffer.iteritems():
            try:
                db.campaign.rating.update(
                    {'adv': key[0], 'adv_int': key[2], 'campaignId': key[1], 'campaignId_int': key[3]},
                    {'$inc': {'impressions': value, 'full_impressions': value}}, True)
            except Exception, ex:
                print ex
                print "campaign_rating_buffer"
                print key, value
        elapsed = (datetime.datetime.now() - elapsed_start_time).seconds
        print "Stop import rating Data in %s second" % (elapsed,)
        if db.previous_error():
            print "Database error", db.previous_error()

    def importClicksFromMongo(self, db):
        u"""Обработка кликов из mongo"""
        elapsed_start_time = datetime.datetime.now()
        # _id последней записи, обработанной скриптом. Если не было обработано ничего, равно None 
        last_processed_id = None
        try:
            last_processed_id = db.config.find_one({'key': 'clicks rating last _id'})['value']
        except:
            last_processed_id = None
        if not isinstance(last_processed_id, bson.objectid.ObjectId):
            last_processed_id = None

        cursor = db['clicks'].find().sort("$natural", pymongo.DESCENDING)
        try:
            end_id = cursor[0]['_id']  # Последний id, который будет обработан в этот раз
        except:
            print "importClicksFromMongo: nothing to do"
            return

        offer_cost = {}
        buffer_click = []
        processed_records = 0
        for x in cursor:

            if last_processed_id <> None and x['_id'] == last_processed_id:
                break
            buffer_click.append(x)

        db.config.update({'key': 'clicks rating last _id'}, {'$set': {'value': end_id}}, True)

        for x in buffer_click:
            processed_records += 1

            if float(x['adload_cost']) > 0.0:
                offer_cost[x['offer']] = x['adload_cost']
            if x['unique']:
                db.offer.update({'guid': x['offer']},
                                {'$inc': {'clicks': 1, 'full_clicks': 1}}, upsert=False)

                db.stats_daily.rating.update({'adv': x['inf'],
                                              'guid': x['offer'],
                                              'campaignId': x['campaignId']},
                                             {'$inc': {'clicks': 1, 'full_clicks': 1}}, upsert=False)

                db.campaign.rating.update({'adv': x['inf'], 'campaignId': x['campaignId']},
                                          {'$inc': {'clicks': 1, 'full_clicks': 1}}, False)

        print "Finished %s records in %s seconds" % (
        processed_records, (datetime.datetime.now() - elapsed_start_time).seconds)

        print "update offer cost -", len(offer_cost)
        for key, value in offer_cost.iteritems():
            db.offer.update({'guid': key}, {'$set': {'cost': value}}, upsert=False)

    def createOfferRating(self, db):
        msg = {}
        campaignIdList = [x['guid'] for x in
                          db.campaign.find({"showConditions.retargeting": False, "status": "working"}, {
                              'guid': 1,
                              '_id': -1
                          })]
        queri = {"campaignId": {"$in": campaignIdList}}
        offers = db.offer.find(queri)
        offer_count = 0
        offer_skip = 0
        date_update = datetime.datetime.now()
        for offer in offers:
            udata = {}
            impressions = offer.get('impressions', 0)
            clicks = offer.get('clicks', 0)
            old_impressions = offer.get('old_impressions', 0)
            old_clicks = offer.get('old_clicks', 0)
            full_impressions = offer.get('full_impressions', 0)
            full_clicks = offer.get('full_clicks', 0)
            offer_cost = offer.get('cost', 0.1)
            if (clicks and impressions) > 0:
                ctr = ((float(clicks) / impressions) * 100)
            else:
                ctr = 0
            if (old_clicks and old_impressions) > 0:
                old_ctr = ((float(old_clicks) / old_impressions) * 100)
            else:
                old_ctr = 0
            if (full_clicks and full_impressions) > 0:
                full_ctr = ((float(full_clicks) / full_impressions) * 100)
            else:
                full_ctr = 0
            if (impressions > 1500):
                rating = (ctr * offer_cost) * 100000
                udata['rating'] = round(rating, 4)
                udata['last_rating_update'] = date_update
                udata['old_impressions'] = impressions
                udata['old_clicks'] = clicks
            if (full_impressions > 1500):
                rating = (full_ctr * offer_cost) * 100000
                udata['full_rating'] = round(rating, 4)
                udata['last_full_rating_update'] = date_update
                offer_count += 1
                msg[offer['guid']] = offer['campaignId']
            if len(udata) > 0:
                db.offer.update({'guid': offer['guid']}, \
                                {'$set': udata}, upsert=False)
        for key, value in msg.iteritems():
            MQ().offer_update(key, value)
        MQ().offer_rating_update()
        print "Created %d rating for offer" % (offer_count)

    def createCampaignRatingForInformers(self, db):
        date_update = datetime.datetime.now()
        costs = defaultdict(list)
        for item in db.offer.find({}, {'campaignId': True, 'cost': True}):
            costs[item['campaignId']].append(item['cost'])
        campaigns = db.campaign.rating.find()
        for campaign in campaigns:
            udata = {}
            impressions = campaign.get('impressions', 0)
            clicks = campaign.get('clicks', 0)
            old_impressions = campaign.get('old_impressions', 0)
            old_clicks = campaign.get('old_clicks', 0)
            full_impressions = campaign.get('full_impressions', 0)
            full_clicks = campaign.get('full_clicks', 0)
            cost = costs.get(campaign['campaignId'], [0.5])
            if len(cost) > 0:
                cost = sum(cost) / len(cost)
            else:
                cost = 0.5
            if (clicks and impressions) > 0:
                ctr = ((float(clicks) / impressions) * 100)
            else:
                ctr = 0
            if (old_clicks and old_impressions) > 0:
                old_ctr = ((float(old_clicks) / old_impressions) * 100)
            else:
                old_ctr = 0
            if (full_clicks and full_impressions) > 0:
                full_ctr = ((float(full_clicks) / full_impressions) * 100)
            else:
                full_ctr = 0
            if (impressions > 1500):
                rating = (ctr * cost) * 100000
                udata['rating'] = round(rating, 4)
                udata['last_rating_update'] = date_update
                udata['old_impressions'] = impressions
                udata['cost'] = cost
                udata['old_clicks'] = clicks

            if (full_impressions > 1500):
                rating = (full_ctr * cost) * 100000
                udata['full_rating'] = round(rating, 4)
                udata['cost'] = cost
                udata['last_full_rating_update'] = date_update

            if len(udata) > 0:
                db.campaign.rating.update({'campaignId': campaign['campaignId'],
                                           'campaignId_int': campaign['campaignId_int'],
                                           'adv': campaign['adv'],
                                           'adv_int': campaign['adv_int']},
                                          {'$set': udata}, upsert=False)
        MQ().campaign_rating_update()

    def createOfferRatingForInformers(self, db):
        offers = db.stats_daily.rating.find()
        date_update = datetime.datetime.now()
        offer_count = 0
        offer_skip = 0
        costs = {}
        msg = {}
        informersBySite = {}
        informersByTitle = {}
        for informer in db.informer.find({}, {'guid': True, 'domain': True, 'title': True}):
            try:
                informersBySite[informer['guid']] = informer.get('domain', 'NOT DOMAIN')
                informersByTitle[informer['guid']] = informer.get('title', 'NOT TITLE')
            except:
                pass
        campaignIdList = []
        campaign = {}
        for x in db.campaign.find({"showConditions.retargeting": False, "status": "working"}, {'guid': 1, 'title': 1, '_id': -1}):
            campaignIdList.append(x['guid'])
            campaign[x['guid']] = x['title']

        queri = {"campaignId": {"$in": campaignIdList}}
        for item in db.offer.find(queri, {'guid': 1, 'cost': 1, 'title': 1, '_id':-1}):
            costs[item['guid']] = [item['cost'], item['title']]

        offers = db.stats_daily.rating.find(queri)
        for offer in offers:
            udata = {}
            impressions = offer.get('impressions', 0)
            clicks = offer.get('clicks', 0)
            old_impressions = offer.get('old_impressions', 0)
            old_clicks = offer.get('old_clicks', 0)
            full_impressions = offer.get('full_impressions', 0)
            full_clicks = offer.get('full_clicks', 0)
            offer_cost, offer_title = costs.get(offer['guid'], [0.5, ''])
            campaignTitle = campaign.get(offer['campaignId'],'')
            if (clicks and impressions) > 0:
                ctr = ((float(clicks) / impressions) * 100)
            else:
                ctr = 0
            if (old_clicks and old_impressions) > 0:
                old_ctr = ((float(old_clicks) / old_impressions) * 100)
            else:
                old_ctr = 0
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
                db.stats_daily.rating.update({'guid': offer['guid'],
                                              'guid_int': offer['guid_int'],
                                              'campaignId': offer['campaignId'],
                                              'campaignId_int': offer['campaignId_int'],
                                              'adv': offer['adv'],
                                              'adv_int': offer['adv_int']},
                                             {'$set': udata}, upsert=False)

        for key, value in msg.iteritems():
            MQ().rating_informer_update(value)
        MQ().informer_rating_update()
        print "Created %d rating for offer-informer" % (offer_count)

    def delete_old_rating_stats(self, db):
        u"""Удаляем старую статистику"""
        offersId = []
        campaignIdList = [x['guid'] for x in db.campaign.find({"showConditions.retargeting": False})]
        informerIdList = [x['guid'] for x in db.informer.find({})]
        print informerIdList
        queri = {"campaignId": {"$in": campaignIdList}}
        for item in db.offer.find(queri, {"guid": 1, "_id": 0}):
            offersId.append(item['guid'])
        a = 0
        i = 0
        y = 0
        d = 0
        z = 0
        a = db.stats_daily.rating.remove({'adv': {'$nin': informerIdList}})
        i = db.stats_daily.rating.remove({'guid': {'$nin': offersId}})
        d = db.stats_daily.rating.remove({'full_rating': 0})
        # Сбрасуем показы и клики в товарах
        y = db.offer.update({'$or': [{'impressions': {'$lte': 0}}, {'impressions': {'$gte': 1500}},
                                     {'impressions': {'$exists': False}}]},
                            {'$set': {'impressions': 0,
                                      'clicks': 0}}, multi=True, w=1)
        # Сбрасуем показы и клики в товарах по рекламному блоку
        z = db.stats_daily.rating.update({'$or': [{'impressions': {'$lte': 0}}, {'impressions': {'$gte': 1500}},
                                                  {'impressions': {'$exists': False}}]},
                                         {'$set': {'impressions': 0,
                                                   'clicks': 0}}, multi=True, w=1)

        print "records deleted"
        print i
        print "records offer/inf 0 rating deleted"
        print d
        print "records inf rating deleted"
        print a
        print "clearn offer imp/click"
        print y
        print "clearn offer/inf imp/click"
        print z

    def trunkete_rating_stats(self, db):
        u"""Обризаем показы"""
        of_im = 1000000
        of_inf_im = 250000
        for item in db.offer.find({'full_impressions': {'$gte': of_im * 4}, 'retargeting': False}):
            full_impressions = item.get('full_impressions', 0)
            full_clicks = item.get('full_clicks', 0)
            delta = full_impressions / of_im
            if (full_clicks and full_impressions) > 0:
                propor = (float(full_clicks) / full_impressions)
            else:
                item['full_impressions'] = of_im * 3
                db.offer.save(item)
                continue

            item['full_impressions'] = of_im * 3
            item['full_clicks'] = int(propor * of_im * 3)
            db.offer.save(item)

        for item in db.stats_daily.rating.find({'full_impressions': {'$gte': of_inf_im * 4}}):
            full_impressions = item.get('full_impressions', 0)
            full_clicks = item.get('full_clicks', 0)
            delta = full_impressions / of_im
            if (full_clicks and full_impressions) > 0:
                propor = (float(full_clicks) / full_impressions)
            else:
                item['full_impressions'] = of_inf_im * 4
                db.stats_daily.rating.save(item)
                continue

            item['full_impressions'] = of_inf_im * 4
            item['full_clicks'] = int(propor * of_inf_im * 4)
            db.stats_daily.rating.save(item)
