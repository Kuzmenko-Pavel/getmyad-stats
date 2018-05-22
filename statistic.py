# -*- coding: utf-8 -*-
import StringIO
import datetime
import ftplib
import socket
from collections import defaultdict

import bson.objectid
import pymongo
import requests
import xlwt
from pymongo.errors import BulkWriteError


class GetmyadStats(object):
    def __init__(self, db, pool):
        self.db = db
        self.pool = pool

    def import_retargeting_track_data(self):
        u"""Mongo worker retargeting track data import"""
        elapsed_start_time = datetime.datetime.now()

        ip_buffer = defaultdict(int)
        processed_records = 0

        for db2 in self.pool:
            try:
                print(db2)

                try:
                    last_processed_id = db2.config.find_one({'key': 'impressions retargeting last _id'})['value']
                except:
                    last_processed_id = None
                if not isinstance(last_processed_id, bson.objectid.ObjectId):
                    last_processed_id = None

                try:
                    cursor = db2['log.retargeting'].find().sort("$natural", pymongo.DESCENDING)
                except Exception as e:
                    print("Cursor ERROR", e)
                    continue
                try:
                    end_id = cursor[0]['_id']  # Последний id, который будет обработан в этот раз
                    print(end_id)
                except Exception as e:
                    print("importImpressionsFromMongo: nothing to do")
                    continue

                try:
                    for x in cursor:
                        try:
                            if last_processed_id is not None and x['_id'] == last_processed_id:
                                break
                            processed_records += 1
                            n = x['dt']
                            dt = datetime.datetime(n.year, n.month, n.day)
                            ip = x.get('ip')
                            if ip:
                                key = (dt, ip)
                                ip_buffer[key] += 1
                        except Exception as e:
                            print("Iteration error", e)
                            pass
                except Exception as e:
                    print("For cursor error", e)
                    pass
                print("read base complite")
                db2.config.update({'key': 'impressions retargeting last _id'}, {'$set': {'value': end_id}}, True)
            except Exception as e:
                print("Worker base error", e)
                pass

        self.db.reset_error_history()

        # operations = []
        # for key, value in ip_buffer.iteritems():
        #     try:
        #         operations.append(
        #             pymongo.UpdateOne(
        #                 {'ip': key[1], 'date': key[0]},
        #                 {'$inc': {'impressions_retargeting': value}},
        #                 upsert=True
        #             )
        #         )
        #     except Exception as ex:
        #         print(ex, "ip_buffer", key, value)
        # try:
        #     self.db.ip.stats.daily.raw.bulk_write(operations, ordered=False)
        # except BulkWriteError as bwe:
        #     print(bwe.details)

        elapsed = (datetime.datetime.now() - elapsed_start_time).seconds
        print('%s seconds, %s records processed. \n' % (elapsed, processed_records))
        if self.db.previous_error():
            print("Database error", self.db.previous_error())

    def importWorkerBlockData(self):
        u"""Mongo worker data import"""
        elapsed_start_time = datetime.datetime.now()

        buffer = defaultdict(lambda: [0, 0])
        ip_buffer = defaultdict(lambda: [0, 0, 0])
        processed_records = 0

        for db2 in self.pool:
            try:
                print(db2)

                try:
                    last_processed_id = db2.config.find_one({'key': 'impressions block last _id'})['value']
                except:
                    last_processed_id = None
                if not isinstance(last_processed_id, bson.objectid.ObjectId):
                    last_processed_id = None

                try:
                    cursor = db2['log.impressions.block'].find().sort("$natural", pymongo.DESCENDING)
                except Exception as e:
                    print("Cursor ERROR", e)
                    continue
                try:
                    end_id = cursor[0]['_id']  # Последний id, который будет обработан в этот раз
                    print(end_id)
                except Exception as e:
                    print("importImpressionsFromMongo: nothing to do")
                    continue

                try:
                    for x in cursor:
                        try:
                            if last_processed_id is not None and x['_id'] == last_processed_id:
                                break
                            n = x['dt']
                            guid = x.get('guid', '')
                            dt = datetime.datetime(n.year, n.month, n.day)
                            key = (dt, guid)

                            garanted = x.get('garanted', False)
                            processed_records += 1
                            if garanted:
                                buffer[key][0] += 1
                            else:
                                buffer[key][1] += 1

                            ip = x.get('ip')
                            if ip:
                                key = (dt, ip)
                                ip_buffer[key][0] += 1
                                if garanted:
                                    ip_buffer[key][1] += 1
                                else:
                                    ip_buffer[key][2] += 1
                        except Exception as e:
                            print("Iteration error", e)
                            pass
                except Exception as e:
                    print("For cursor error", e)
                    pass
                print("read base complite")
                db2.config.update({'key': 'impressions block last _id'}, {'$set': {'value': end_id}}, True)
            except Exception as e:
                print("Worker base error", e)
                pass

        self.db.reset_error_history()
        operations = []

        for key, value in buffer.iteritems():
            try:
                operations.append(
                    pymongo.UpdateOne(
                        {'guid': key[1], 'date': key[0]},
                        {'$inc': {'impressions_block': value[0], 'impressions_block_not_valid': value[1]}},
                        upsert=True)
                )
            except Exception as ex:
                print(ex, "buffer", key, value)

        try:
            self.db.stats.daily.raw.bulk_write(operations, ordered=False)
        except BulkWriteError as bwe:
            print(bwe.details)

        # operations = []
        #
        # for key, value in ip_buffer.iteritems():
        #     try:
        #         operations.append(
        #             pymongo.UpdateOne(
        #                 {'ip': key[1], 'date': key[0]},
        #                 {'$inc': {
        #                     'impressions_block': value[0],
        #                     'impressions_block_valid': value[1],
        #                     'impressions_block_not_valid': value[2],
        #                 }},
        #                 upsert=True)
        #         )
        #     except Exception as ex:
        #         print(ex, "ip_buffer", key, value)
        #
        # try:
        #     self.db.ip.stats.daily.raw.bulk_write(operations, ordered=False)
        # except BulkWriteError as bwe:
        #     print(bwe.details)

        elapsed = (datetime.datetime.now() - elapsed_start_time).seconds
        print('%s seconds, %s records processed. \n' % (elapsed, processed_records))
        if 'log.statisticProcess' not in self.db.collection_names():
            self.db.create_collection('log.statisticProcess',
                                      capped=True, size=50000000, max=10000)
        self.db.log.statisticProcess.insert_one({'dt': datetime.datetime.now(),
                                                 'impressions block': {
                                                     'count': processed_records,
                                                     'elapsed_time': (
                                                         datetime.datetime.now() - elapsed_start_time).seconds
                                                 },
                                                 'clicks': 'not processed',
                                                 'srv': '2'})
        if self.db.previous_error():
            print("Database error", self.db.previous_error())

    def importWorkerOfferData(self):
        u"""Mongo worker data import"""
        elapsed_start_time = datetime.datetime.now()

        buffer = defaultdict(lambda: [0, 0])
        worker_stats = defaultdict(int)
        ip_buffer = defaultdict(lambda: [0, 0, 0, 0, 0])
        processed_records = 0
        processed_social_records = 0
        processed_paymend_records = 0

        for db2 in self.pool:
            try:
                print(db2)
                try:
                    last_processed_id = db2.config.find_one({'key': 'impressions offer last _id'})['value']
                except:
                    last_processed_id = None
                if not isinstance(last_processed_id, bson.objectid.ObjectId):
                    last_processed_id = None

                try:
                    cursor = db2['log.impressions'].find({}, {'title': False, 'campaignTitle': False}).sort("$natural",
                                                                                                            pymongo.DESCENDING)
                except Exception as e:
                    print("Cursor ERROR", e)
                    continue
                try:
                    end_id = cursor[0]['_id']  # Последний id, который будет обработан в этот раз
                    print(end_id)
                except Exception as e:
                    print("importImpressionsFromMongo: nothing to do")
                    continue

                try:
                    for x in cursor:
                        try:
                            if last_processed_id is not None and x['_id'] == last_processed_id:
                                break
                            n = x['dt']
                            dt = datetime.datetime(n.year, n.month, n.day)

                            key = (x['inf'].lower(), dt)

                            if not x.get('test', False):
                                if x.get('social', False):
                                    processed_social_records += 1
                                    processed_records += 1
                                    buffer[key][1] += 1
                                else:
                                    processed_paymend_records += 1
                                    processed_records += 1
                                    buffer[key][0] += 1

                            branch = x.get('branch', 'NOT')
                            ip = x.get('ip')
                            if ip:
                                key = (dt, ip)
                                ip_buffer[key][0] += 1
                                if x.get('social', False):
                                    ip_buffer[key][2] += 1
                                else:
                                    if branch == 'NL31':
                                        ip_buffer[key][3] += 1
                                    elif branch == 'NL32':
                                        ip_buffer[key][4] += 1
                                    else:
                                        ip_buffer[key][1] += 1

                            stats_key = (branch, dt, x.get('conformity', 'NOT'))
                            stats_key_all = (branch, dt, 'ALL')
                            worker_stats[stats_key] += 1
                            worker_stats[stats_key_all] += 1
                        except Exception as e:
                            print("Iteration error", e)
                            pass
                except Exception as e:
                    print("For cursor error", e)
                    pass
                print("read base complite")
                db2.config.update({'key': 'impressions offer last _id'}, {'$set': {'value': end_id}}, True)
            except Exception as e:
                print("Worker base error", e)
                pass

        self.db.reset_error_history()

        operations = []

        for key, value in buffer.iteritems():
            try:
                operations.append(
                    pymongo.UpdateOne({'guid': key[0],
                                       'date': key[1]},
                                      {'$inc':
                                          {
                                              'impressions': value[0],
                                              'social_impressions': value[1]
                                          }},
                                      upsert=False)
                )
            except Exception as ex:
                print(ex, "buffer", key, value)

        try:
            self.db.stats.daily.raw.bulk_write(operations, ordered=False)
        except BulkWriteError as bwe:
            print(bwe.details)

        # operations = []
        # for key, value in ip_buffer.iteritems():
        #     try:
        #         operations.append(
        #             pymongo.UpdateOne({'ip': key[1],
        #                                'date': key[0]},
        #                               {'$inc':
        #                                   {
        #                                       'impressions': value[0],
        #                                       'place_impressions': value[1],
        #                                       'social_impressions': value[2],
        #                                       'retargeting_impressions': value[3],
        #                                       'recommended_impressions': value[4]
        #                                   }},
        #                               upsert=True)
        #         )
        #     except Exception as ex:
        #         print(ex, "ip_buffer", key, value)
        #
        # try:
        #     self.db.ip.stats.daily.raw.bulk_write(operations, ordered=False)
        # except BulkWriteError as bwe:
        #     print(bwe.details)

        operations = []
        for key, value in worker_stats.iteritems():
            try:
                operations.append(
                    pymongo.UpdateOne({'date': key[1]},
                                      {'$inc': {(str(key[0]) + '.' + str(key[2])): value}}, upsert=True)
                )
            except Exception as ex:
                print(ex, "worker_stats", key, value)

        try:
            self.db.worker_stats.bulk_write(operations, ordered=False)
        except BulkWriteError as bwe:
            print(bwe.details)

        elapsed = (datetime.datetime.now() - elapsed_start_time).seconds
        print('%s seconds, %s records processed. From this tiser %s social records, tiser %s paymend record' %
              (elapsed, processed_records, processed_social_records, processed_paymend_records))
        if 'log.statisticProcess' not in self.db.collection_names():
            self.db.create_collection('log.statisticProcess',
                                      capped=True, size=50000000, max=10000)
        self.db.log.statisticProcess.insert_one({'dt': datetime.datetime.now(),
                                                 'impressions offer': {
                                                     'count': processed_records,
                                                     'elapsed_time':
                                                         (datetime.datetime.now() - elapsed_start_time).seconds
                                                 },
                                                 'clicks': 'not processed',
                                                 'srv': '2'})
        if self.db.previous_error():
            print("Database error", self.db.previous_error())

    def importClicksFromMongo(self):
        u"""Обработка кликов из mongo"""
        elapsed_start_time = datetime.datetime.now()
        # _id последней записи, обработанной скриптом. Если не было обработано ничего, равно None 
        last_processed_id = None
        try:
            last_processed_id = self.db.config.find_one({'key': 'clicks last _id'})['value']
        except:
            last_processed_id = None
        if not isinstance(last_processed_id, bson.objectid.ObjectId):
            last_processed_id = None

        cursor = self.db['clicks'].find().sort("$natural", pymongo.DESCENDING)
        try:
            end_id = cursor[0]['_id']  # Последний id, который будет обработан в этот раз
        except:
            print("importClicksFromMongo: nothing to do")
            return

        buffer_click = []
        processed_records = 0
        for x in cursor:

            if last_processed_id is not None and x['_id'] == last_processed_id:
                break
            buffer_click.append(x)

        self.db.config.update({'key': 'clicks last _id'}, {'$set': {'value': end_id}}, upsert=True)

        for x in buffer_click:
            processed_records += 1
            if x.get('social', False):
                self.db.stats.daily.raw.update({'guid': x['inf'],
                                                'date': datetime.datetime.fromordinal(x['dt'].toordinal())},
                                               {'$inc': {'social_clicks': 1,
                                                         'view_seconds': abs(x.get('view_seconds', 0)),
                                                         'social_clicksUnique': 1 if x['unique'] else 0,
                                                         'adload_cost': x.get('adload_cost', 0),
                                                         'income': x.get('income', 0),
                                                         'totalCost': x['cost']}}, upsert=True)
            else:
                self.db.stats.daily.raw.update({'guid': x['inf'],
                                                'date': datetime.datetime.fromordinal(x['dt'].toordinal())},
                                               {'$inc': {'clicks': 1,
                                                         'view_seconds': abs(x.get('view_seconds', 0)),
                                                         'clicksUnique': 1 if x['unique'] else 0,
                                                         'adload_cost': x.get('adload_cost', 0),
                                                         'income': x.get('income', 0),
                                                         'totalCost': x['cost']}}, upsert=True)
            if len(x.get('conformity', '')) > 0:
                skey = (str(x.get('branch', 'L0')) + '.C' + str(x['conformity']))
            else:
                skey = (str(x.get('branch', 'L0')) + '.CNONE')
            self.db.worker_stats.update({'date': datetime.datetime.fromordinal(x['dt'].toordinal())},
                                        {'$inc': {skey: 1,
                                                  (str(x.get('branch', 'L0')) + '.CALL'): 1}}, upsert=False)
            ip_data = {}
            if x.get('social', False):
                ip_data['social_clicks'] = 1
            else:
                branch = x.get('branch', 'L0')
                if branch == 'NL31':
                    ip_data['retargeting_clicks'] = 1
                elif branch == 'NL32':
                    ip_data['recommended_clicks'] = 1
                else:
                    ip_data['place_clicks'] = 1
            ip_data['all_clicks'] = 1
            ip_data['unique_clicks'] = 1 if x['unique'] else 0

            # self.db.ip.stats.daily.raw.update({
            #     'ip': x['ip'],
            #     'date': datetime.datetime.fromordinal(x['dt'].toordinal())
            # }, {
            #     '$inc': ip_data
            # })

        print("Finished %s records in %s seconds" %
              (processed_records, (datetime.datetime.now() - elapsed_start_time).seconds))
        result_clicks = {'count': processed_records,
                         'elapsed_time': (datetime.datetime.now() - elapsed_start_time).seconds}
        if 'log.statisticProcess' not in self.db.collection_names():
            self.db.create_collection('log.statisticProcess',
                                      capped=True, size=50000000, max=10000)
        self.db.log.statisticProcess.insert_one({'dt': datetime.datetime.now(),
                                                 'clicks': result_clicks,
                                                 'srv': socket.gethostname()})
        # Обновляем время обработки статистики
        self.db.config.update({'key': 'last stats_daily update date'},
                              {'$set': {'value': datetime.datetime.now()}}, upsert=True)

    def processMongoStats(self, date):
        date = datetime.datetime(date.year, date.month, date.day, 0, 0)
        informersBySite = {}
        informersByUsers = {}
        informersByTitle = {}
        informerList = []
        for informer in self.db.informer.find({}, {'guid': True, 'domain': True, 'user': True, 'title': True}):
            try:
                userGuid = self.db.users.find_one({"login": informer.get('user', 'NOT DOMAIN')}, {'guid': 1, '_id': 0})
                domainGuid = None
                for domains in self.db.domain.find({"login": informer.get('user', 'NOT DOMAIN')},
                                                   {'domains': 1, '_id': 0}):
                    for key, value in domains['domains'].iteritems():
                        if value == informer.get('domain', 'NOT DOMAIN'):
                            domainGuid = key
                informersBySite[informer['guid']] = (informer.get('domain', 'NOT DOMAIN'), domainGuid)
                informersByUsers[informer['guid']] = (informer.get('user', 'NOT DOMAIN'), userGuid.get('guid', ''))
                informersByTitle[informer['guid']] = informer.get('title', 'NOT DOMAIN')
                informerList.append(informer['guid'])
            except:
                pass

        pipeline = [
            {'$match':
                {
                    'date': {'$gte': date, '$lt': date + datetime.timedelta(days=1)}
                }
            },
            {'$group':
                {
                    '_id': {
                        'date': '$date',
                        'guid': '$guid'
                    },
                    'totalCost': {'$sum': '$totalCost'},
                    'adload_cost': {'$sum': '$adload_cost'},
                    'income': {'$sum': '$income'},
                    'impressions_block': {'$sum': '$impressions_block'},
                    'impressions_block_not_valid': {'$sum': '$impressions_block_not_valid'},
                    'impressions': {'$sum': '$impressions'},
                    'clicks': {'$sum': '$clicks'},
                    'clicksUnique': {'$sum': '$clicksUnique'},
                    'social_impressions': {'$sum': '$social_impressions'},
                    'social_clicks': {'$sum': '$social_clicks'},
                    'social_clicksUnique': {'$sum': '$social_clicksUnique'},
                    'view_seconds': {'$sum': '$view_seconds'},
                }
            }
        ]
        cursor = self.db.stats.daily.raw.aggregate(pipeline=pipeline, allowDiskUse=True, useCursor=True)
        bulk = self.db.stats.daily.adv.initialize_unordered_bulk_op()
        for inf in cursor:
            guid = inf['_id']['guid']
            if guid not in informerList:
                print("Not found informer %s" % guid)
                continue

            date = inf['_id']['date']
            domain = informersBySite.get(guid, 'NOT DOMAIN')[0]
            domain_guid = informersBySite.get(guid, 'NOT DOMAIN')[1]
            user = informersByUsers.get(guid, 'not user')[0]
            user_guid = informersByUsers.get(guid, 'not user')[1]
            title = informersByTitle.get(guid, 'NOT TITLE')
            totalCost = inf['totalCost']
            adload_cost = inf['adload_cost']
            income = inf['income']
            clicks = inf['clicks']
            social_clicks = inf['social_clicks']
            view_seconds = inf['view_seconds']

            impressions = int(inf['impressions'])
            social_impressions = int(inf['social_impressions'])
            impressions_block = int(inf['impressions_block'])
            impressions_block_not_valid = int(inf['impressions_block_not_valid'])

            difference_impressions_block = 100.0 * impressions_block / impressions_block_not_valid if (
                impressions_block_not_valid > 0 and impressions_block_not_valid > impressions_block) else 100.0
            clicksUnique = int(inf['clicksUnique'])
            social_clicksUnique = int(inf['social_clicksUnique'])
            ctr_impressions_block = 100.0 * clicksUnique / impressions_block if (
                clicksUnique > 0 and impressions_block > 0) else 0
            ctr_impressions = 100.0 * clicksUnique / impressions if (clicksUnique > 0 and impressions > 0) else 0
            ctr_social_impressions = 100.0 * social_clicksUnique / social_impressions if (
                social_clicksUnique > 0 and social_impressions > 0) else 0
            ctr_difference_impressions = 100.0 * ctr_social_impressions / ctr_impressions if (
                ctr_social_impressions > 0 and ctr_impressions > 0) else 0

            bulk.find({'adv': guid, 'date': date}).upsert().update_one(
                {'$set': {'domain': domain,
                          'domain_guid': domain_guid,
                          'user': user,
                          'user_guid': user_guid,
                          'title': title,
                          'impressions_block': impressions_block,
                          'impressions_block_not_valid': impressions_block_not_valid,
                          'difference_impressions_block': difference_impressions_block,
                          'totalCost': totalCost,
                          'adload_cost': adload_cost,
                          'income': income,
                          'impressions': impressions,
                          'clicks': clicks,
                          'clicksUnique': clicksUnique,
                          'social_impressions': social_impressions,
                          'social_clicks': social_clicks,
                          'social_clicksUnique': social_clicksUnique,
                          'ctr_impressions_block': ctr_impressions_block,
                          'ctr_impressions': ctr_impressions,
                          'ctr_social_impressions': ctr_social_impressions,
                          'ctr_difference_impressions': ctr_difference_impressions,
                          'view_seconds': view_seconds
                          }
                 }
            )

        # Обновляем время обработки статистики
        try:
            bulk.execute()
        except BulkWriteError as e:
            print(e.details)
        self.db.config.update({'key': 'last stats_daily update date'},
                              {'$set': {'value': datetime.datetime.now()}}, upsert=True)

    def agregateStatDailyDomain(self, date):
        u"""Составляет общую статистику по доменам с разбивкой по датам.
            Данные используються в менеджеровском акаунте для обшей статистики
            ``date`` --- дата, на которую считать данные. Может быть типа datetime или date"""
        assert isinstance(date, (datetime.datetime, datetime.date))
        date = datetime.datetime(date.year, date.month, date.day, 0, 0)

        pipeline = [
            {'$match':
                {
                    'date': {'$gte': date, '$lt': date + datetime.timedelta(days=1)}
                }
            },
            {'$group':
                {
                    '_id': {
                        'date': '$date',
                        'domain': '$domain',
                        'domain_guid': '$domain_guid'
                    },
                    'user': {'$last': '$user'},
                    'user_guid': {'$last': '$user_guid'},
                    'totalCost': {'$sum': '$totalCost'},
                    'adload_cost': {'$sum': '$adload_cost'},
                    'income': {'$sum': '$income'},
                    'impressions_block': {'$sum': '$impressions_block'},
                    'impressions_block_not_valid': {'$sum': '$impressions_block_not_valid'},
                    'impressions': {'$sum': '$impressions'},
                    'clicks': {'$sum': '$clicks'},
                    'clicksUnique': {'$sum': '$clicksUnique'},
                    'social_impressions': {'$sum': '$social_impressions'},
                    'social_clicks': {'$sum': '$social_clicks'},
                    'social_clicksUnique': {'$sum': '$social_clicksUnique'},
                    'view_seconds': {'$sum': '$view_seconds'}
                }
            }
        ]

        cursor = self.db.stats.daily.adv.aggregate(pipeline=pipeline, allowDiskUse=True, useCursor=True)
        bulk = self.db.stats.daily.domain.initialize_unordered_bulk_op()
        for x in cursor:
            date = x['_id']['date']
            domain = x['_id']['domain']
            domain_guid = x['_id']['domain_guid']
            impressions_block = int(x['impressions_block'])
            impressions_block_not_valid = int(x['impressions_block_not_valid'])
            difference_impressions_block = 100.0 * impressions_block / impressions_block_not_valid if (
                impressions_block_not_valid > 0 and impressions_block_not_valid > impressions_block) else 100.0
            impressions = int(x['impressions'])
            social_impressions = int(x['social_impressions'])
            clicksUnique = int(x['clicksUnique'])
            social_clicksUnique = int(x['social_clicksUnique'])
            ctr_impressions_block = 100.0 * clicksUnique / impressions_block if (
                clicksUnique > 0 and impressions_block > 0) else 0
            ctr_impressions = 100.0 * clicksUnique / impressions if (clicksUnique > 0 and impressions > 0) else 0
            ctr_social_impressions = 100.0 * social_clicksUnique / social_impressions if (
                social_clicksUnique > 0 and social_impressions > 0) else 0
            ctr_difference_impressions = 100.0 * ctr_social_impressions / ctr_impressions if (
                ctr_social_impressions > 0 and ctr_impressions > 0) else 0
            bulk.find({'domain': domain, 'date': date, 'domain_guid': domain_guid}).upsert().update_one(
                {'$set': {'user': x['user'],
                          'user_guid': x['user_guid'],
                          'totalCost': x['totalCost'],
                          'adload_cost': x['adload_cost'],
                          'income': x['income'],
                          'impressions_block': impressions_block,
                          'impressions_block_not_valid': impressions_block_not_valid,
                          'difference_impressions_block': difference_impressions_block,
                          'impressions': impressions,
                          'clicks': x['clicks'],
                          'clicksUnique': clicksUnique,
                          'social_impressions': social_impressions,
                          'social_clicks': x['social_clicks'],
                          'social_clicksUnique': social_clicksUnique,
                          'ctr_impressions_block': ctr_impressions_block,
                          'ctr_impressions': ctr_impressions,
                          'ctr_social_impressions': ctr_social_impressions,
                          'ctr_difference_impressions': ctr_difference_impressions,
                          'view_seconds': x['view_seconds']
                          }}
            )

        try:
            bulk.execute()
        except BulkWriteError as e:
            print(e.details)

    def agregateStatDailyUser(self, date):
        u"""Составляет общую статистику по доменам с разбивкой по датам.
            Данные используються в менеджеровском акаунте для обшей статистики
            ``date`` --- дата, на которую считать данные. Может быть типа datetime или date"""
        assert isinstance(date, (datetime.datetime, datetime.date))
        date = datetime.datetime(date.year, date.month, date.day, 0, 0)
        pipeline = [
            {'$match':
                {
                    'date': {'$gte': date, '$lt': date + datetime.timedelta(days=1)}
                }
            },
            {'$group':
                {
                    '_id': {
                        'date': '$date',
                        'user': '$user',
                        'user_guid': '$user_guid'
                    },
                    'totalCost': {'$sum': '$totalCost'},
                    'adload_cost': {'$sum': '$adload_cost'},
                    'income': {'$sum': '$income'},
                    'impressions_block': {'$sum': '$impressions_block'},
                    'impressions_block_not_valid': {'$sum': '$impressions_block_not_valid'},
                    'impressions': {'$sum': '$impressions'},
                    'clicks': {'$sum': '$clicks'},
                    'clicksUnique': {'$sum': '$clicksUnique'},
                    'social_impressions': {'$sum': '$social_impressions'},
                    'social_clicks': {'$sum': '$social_clicks'},
                    'social_clicksUnique': {'$sum': '$social_clicksUnique'},
                    'view_seconds': {'$sum': '$view_seconds'}
                }
            }
        ]
        cursor = self.db.stats.daily.domain.aggregate(pipeline=pipeline, allowDiskUse=True, useCursor=True)
        bulk = self.db.stats.daily.user.initialize_unordered_bulk_op()

        for x in cursor:
            date = x['_id']['date']
            user = x['_id']['user']
            user_guid = x['_id']['user_guid']
            impressions_block = int(x['impressions_block'])
            impressions_block_not_valid = int(x['impressions_block_not_valid'])
            difference_impressions_block = 100.0 * impressions_block / impressions_block_not_valid if (
                impressions_block_not_valid > 0 and impressions_block_not_valid > impressions_block) else 100.0
            impressions = int(x['impressions'])
            social_impressions = int(x['social_impressions'])
            clicksUnique = int(x['clicksUnique'])
            social_clicksUnique = int(x['social_clicksUnique'])
            ctr_impressions_block = 100.0 * clicksUnique / impressions_block if (
                clicksUnique > 0 and impressions_block > 0) else 0
            ctr_impressions = 100.0 * clicksUnique / impressions if (clicksUnique > 0 and impressions > 0) else 0
            ctr_social_impressions = 100.0 * social_clicksUnique / social_impressions if (
                social_clicksUnique > 0 and social_impressions > 0) else 0
            ctr_difference_impressions = 100.0 * ctr_social_impressions / ctr_impressions if (
                ctr_social_impressions > 0 and ctr_impressions > 0) else 0

            bulk.find({'user': user, 'date': date, 'user_guid': user_guid}).upsert().update_one(
                {'$set': {'totalCost': x['totalCost'],
                          'adload_cost': x['adload_cost'],
                          'income': x['income'],
                          'impressions_block': impressions_block,
                          'impressions_block_not_valid': impressions_block_not_valid,
                          'difference_impressions_block': difference_impressions_block,
                          'impressions': impressions,
                          'clicks': x['clicks'],
                          'clicksUnique': clicksUnique,
                          'social_impressions': social_impressions,
                          'social_clicks': x['social_clicks'],
                          'social_clicksUnique': social_clicksUnique,
                          'ctr_impressions_block': ctr_impressions_block,
                          'ctr_impressions': ctr_impressions,
                          'ctr_social_impressions': ctr_social_impressions,
                          'ctr_difference_impressions': ctr_difference_impressions,
                          'view_seconds': x['view_seconds']
                          }}

            )

        try:
            bulk.execute()
        except BulkWriteError as e:
            print(e.details)

    def agregateStatDailyAll(self, date):
        u"""Составляет общую статистику по доменам с разбивкой по датам.
            Данные используються в менеджеровском акаунте для обшей статистики
            ``date`` --- дата, на которую считать данные. Может быть типа datetime или date"""
        assert isinstance(date, (datetime.datetime, datetime.date))
        date = datetime.datetime(date.year, date.month, date.day, 0, 0)

        pipeline = [
            {'$match':
                {
                    'date': {'$gte': date, '$lt': date + datetime.timedelta(days=1)}
                }
            },
            {'$group':
                {
                    '_id': {
                        'date': '$date'
                    },
                    'totalCost': {'$sum': '$totalCost'},
                    'adload_cost': {'$sum': '$adload_cost'},
                    'income': {'$sum': '$income'},
                    'impressions_block': {'$sum': '$impressions_block'},
                    'impressions_block_not_valid': {'$sum': '$impressions_block_not_valid'},
                    'impressions': {'$sum': '$impressions'},
                    'clicks': {'$sum': '$clicks'},
                    'clicksUnique': {'$sum': '$clicksUnique'},
                    'social_impressions': {'$sum': '$social_impressions'},
                    'social_clicks': {'$sum': '$social_clicks'},
                    'social_clicksUnique': {'$sum': '$social_clicksUnique'},
                    'view_seconds': {'$sum': '$view_seconds'}
                }
            }
        ]
        cursor = self.db.stats.daily.user.aggregate(pipeline=pipeline, allowDiskUse=True, useCursor=True)
        bulk = self.db.stats.daily.all.initialize_unordered_bulk_op()

        for x in cursor:
            date = x['_id']['date']
            impressions_block = int(x['impressions_block'])
            impressions_block_not_valid = int(x['impressions_block_not_valid'])
            difference_impressions_block = 100.0 * impressions_block / impressions_block_not_valid if (
                impressions_block_not_valid > 0 and impressions_block_not_valid > impressions_block) else 100.0
            impressions = int(x['impressions'])
            social_impressions = int(x['social_impressions'])
            clicksUnique = int(x['clicksUnique'])
            social_clicksUnique = int(x['social_clicksUnique'])
            ctr_impressions_block = 100.0 * clicksUnique / impressions_block if (
                clicksUnique > 0 and impressions_block > 0) else 0
            ctr_impressions = 100.0 * clicksUnique / impressions if (clicksUnique > 0 and impressions > 0) else 0
            ctr_social_impressions = 100.0 * social_clicksUnique / social_impressions if (
                social_clicksUnique > 0 and social_impressions > 0) else 0
            ctr_difference_impressions = 100.0 * ctr_social_impressions / ctr_impressions if (
                ctr_social_impressions > 0 and ctr_impressions > 0) else 0
            bulk.find({'date': date}).upsert().update_one(
                {'$set': {'totalCost': x['totalCost'],
                          'adload_cost': x['adload_cost'],
                          'income': x['income'],
                          'impressions_block': impressions_block,
                          'impressions_block_not_valid': impressions_block_not_valid,
                          'difference_impressions_block': difference_impressions_block,
                          'impressions': impressions,
                          'clicks': x['clicks'],
                          'clicksUnique': clicksUnique,
                          'social_impressions': social_impressions,
                          'social_clicks': x['social_clicks'],
                          'social_clicksUnique': social_clicksUnique,
                          'ctr_impressions_block': ctr_impressions_block,
                          'ctr_impressions': ctr_impressions,
                          'ctr_social_impressions': ctr_social_impressions,
                          'ctr_difference_impressions': ctr_difference_impressions,
                          'view_seconds': x['view_seconds']
                          }}
            )

        try:
            bulk.execute()
        except BulkWriteError as e:
            print(e.details)

    def agregateStatUserSummary(self, date):
        u"""Составляет общую статистику по доменам с разбивкой по датам.
            Данные используються в менеджеровском акаунте для обшей статистики
            ``date`` --- дата, на которую считать данные. Может быть типа datetime или date"""
        assert isinstance(date, (datetime.datetime, datetime.date))
        date = datetime.datetime(date.year, date.month, date.day, 0, 0)

        data = defaultdict(lambda: defaultdict(int))

        condition1 = {'date': {'$gte': date, '$lt': date + datetime.timedelta(days=1)}}
        condition2 = {'date': {'$gte': date - datetime.timedelta(days=1), '$lt': date}}
        condition3 = {'date': {'$gte': date - datetime.timedelta(days=2), '$lt': date - datetime.timedelta(days=1)}}
        condition7 = {'date': {'$gte': date - datetime.timedelta(days=7), '$lt': date + datetime.timedelta(days=1)}}
        condition30 = {'date': {'$gte': date - datetime.timedelta(days=30), '$lt': date + datetime.timedelta(days=1)}}
        condition365 = {'date': {'$gte': date - datetime.timedelta(days=365), '$lt': date + datetime.timedelta(days=1)}}

        for x in self.db.users.find({}, {'login': 1, 'registrationDate': 1, '_id': 0}):
            data[x['login']].update({'registrationDate': x.get('registrationDate'),
                                     'totalCost': 0,
                                     'impressions_block': 0,
                                     'impressions': 0,
                                     'clicks': 0,
                                     'clicksUnique': 0,
                                     'social_impressions': 0,
                                     'social_clicks': 0,
                                     'social_clicksUnique': 0,
                                     'totalCost_2': 0,
                                     'impressions_block_2': 0,
                                     'impressions_2': 0,
                                     'clicks_2': 0,
                                     'clicksUnique_2': 0,
                                     'social_impressions_2': 0,
                                     'social_clicks_2': 0,
                                     'social_clicksUnique_2': 0,
                                     'totalCost_3': 0,
                                     'impressions_block_3': 0,
                                     'impressions_3': 0,
                                     'clicks_3': 0,
                                     'clicksUnique_3': 0,
                                     'social_impressions_3': 0,
                                     'social_clicks_3': 0,
                                     'social_clicksUnique_3': 0,
                                     'totalCost_7': 0,
                                     'impressions_block_7': 0,
                                     'impressions_7': 0,
                                     'clicks_7': 0,
                                     'clicksUnique_7': 0,
                                     'social_impressions_7': 0,
                                     'social_clicks_7': 0,
                                     'social_clicksUnique_7': 0,
                                     'totalCost_30': 0,
                                     'impressions_block_30': 0,
                                     'impressions_30': 0,
                                     'clicks_30': 0,
                                     'clicksUnique_30': 0,
                                     'social_impressions_30': 0,
                                     'social_clicks_30': 0,
                                     'social_clicksUnique_30': 0,
                                     'totalCost_365': 0,
                                     'impressions_block_365': 0,
                                     'impressions_365': 0,
                                     'clicks_365': 0,
                                     'clicksUnique_365': 0,
                                     'social_impressions_365': 0,
                                     'social_clicks_365': 0,
                                     'social_clicksUnique_365': 0
                                     })

        reduce = '''
                function(o, p) {
                   p.totalCost += o.totalCost || 0;
                   p.impressions_block += o.impressions_block || 0;
                   p.impressions += o.impressions || 0;
                   p.clicks += o.clicks || 0;
                   p.clicksUnique += o.clicksUnique || 0;
                   p.social_impressions += o.social_impressions || 0;
                   p.social_clicks += o.social_clicks || 0;
                   p.social_clicksUnique += o.social_clicksUnique || 0;
                }'''
        initial = {'totalCost': 0,
                   'impressions_block': 0,
                   'impressions': 0,
                   'clicks': 0,
                   'clicksUnique': 0,
                   'social_impressions': 0,
                   'social_clicks': 0,
                   'social_clicksUnique': 0
                   }
        cur1 = self.db.stats.daily.user.group(
            key=['user'],
            condition=condition1,
            reduce=reduce,
            initial=initial
        )
        cur2 = self.db.stats.daily.user.group(
            key=['user'],
            condition=condition2,
            reduce=reduce,
            initial=initial
        )
        cur3 = self.db.stats.daily.user.group(
            key=['user'],
            condition=condition3,
            reduce=reduce,
            initial=initial
        )
        cur7 = self.db.stats.daily.user.group(
            key=['user'],
            condition=condition7,
            reduce=reduce,
            initial=initial
        )
        cur30 = self.db.stats.daily.user.group(
            key=['user'],
            condition=condition30,
            reduce=reduce,
            initial=initial
        )
        cur365 = self.db.stats.daily.user.group(
            key=['user'],
            condition=condition365,
            reduce=reduce,
            initial=initial
        )

        for x in cur1:
            data[x['user']].update({'totalCost': x['totalCost'],
                                    'impressions_block': x['impressions_block'],
                                    'impressions': x['impressions'],
                                    'clicks': x['clicks'],
                                    'clicksUnique': x['clicksUnique'],
                                    'social_impressions': x['social_impressions'],
                                    'social_clicks': x['social_clicks'],
                                    'social_clicksUnique': x['social_clicksUnique'],
                                    })

        for x in cur2:
            data[x['user']].update({'totalCost_2': x['totalCost'],
                                    'impressions_block_2': x['impressions_block'],
                                    'impressions_2': x['impressions'],
                                    'clicks_2': x['clicks'],
                                    'clicksUnique_2': x['clicksUnique'],
                                    'social_impressions_2': x['social_impressions'],
                                    'social_clicks_2': x['social_clicks'],
                                    'social_clicksUnique_2': x['social_clicksUnique']
                                    })

        for x in cur3:
            data[x['user']].update({'totalCost_3': x['totalCost'],
                                    'impressions_block_3': x['impressions_block'],
                                    'impressions_3': x['impressions'],
                                    'clicks_3': x['clicks'],
                                    'clicksUnique_3': x['clicksUnique'],
                                    'social_impressions_3': x['social_impressions'],
                                    'social_clicks_3': x['social_clicks'],
                                    'social_clicksUnique_3': x['social_clicksUnique']
                                    })

        for x in cur7:
            data[x['user']].update({'totalCost_7': x['totalCost'],
                                    'impressions_block_7': x['impressions_block'],
                                    'impressions_7': x['impressions'],
                                    'clicks_7': x['clicks'],
                                    'clicksUnique_7': x['clicksUnique'],
                                    'social_impressions_7': x['social_impressions'],
                                    'social_clicks_7': x['social_clicks'],
                                    'social_clicksUnique_7': x['social_clicksUnique']
                                    })

        for x in cur30:
            data[x['user']].update({'totalCost_30': x['totalCost'],
                                    'impressions_block_30': x['impressions_block'],
                                    'impressions_30': x['impressions'],
                                    'clicks_30': x['clicks'],
                                    'clicksUnique_30': x['clicksUnique'],
                                    'social_impressions_30': x['social_impressions'],
                                    'social_clicks_30': x['social_clicks'],
                                    'social_clicksUnique_30': x['social_clicksUnique']
                                    })

        for x in cur365:
            data[x['user']].update({'totalCost_365': x['totalCost'],
                                    'impressions_block_365': x['impressions_block'],
                                    'impressions_365': x['impressions'],
                                    'clicks_365': x['clicks'],
                                    'clicksUnique_365': x['clicksUnique'],
                                    'social_impressions_365': x['social_impressions'],
                                    'social_clicks_365': x['social_clicks'],
                                    'social_clicksUnique_365': x['social_clicksUnique']
                                    })

        # Доход
        inc = {}
        income = self.db.stats.daily.user.group(['user'],
                                                {},
                                                {'sum': 0},
                                                'function(o,p) {p.sum += (o.totalCost || 0); }')
        for item in income:
            inc[item.get('user')] = item.get('sum', 0.0)
        # Сумма выведенных денег
        outc = {}
        outcome = self.db.money_out_request.group(['user.login'],
                                                  {'approved': True},
                                                  {'sum': 0},
                                                  'function(o,p) {p.sum += (o.summ || 0); }')
        for item in outcome:
            outc[item.get('user.login')] = item.get('sum', 0.0)
        for key, value in inc.iteritems():
            data[key].update({'summ': (float(value) - float(outc.get(key, 0.0)))})

        pipeline = [
            {'$match':
                 {'date': date}
             },
            {'$group': {
                '_id': {'user': '$user', 'domain': '$domain'},
                'c': {'$sum': '$clicks'},
                'i': {'$sum': '$impressions'}
            },
            },
            {'$group': {
                '_id': '$_id.user',
                'count': {'$sum': {"$cond": [
                    {'$or': [
                        {"$gt": ["$c", 0]},
                        {"$gte": ["$i", 100]}
                    ]},
                    1,
                    0
                ]}}
            },
            }
        ]
        domain_activity = defaultdict(int)
        cursor = self.db.stats.daily.domain.aggregate(pipeline=pipeline, cursor={})
        for i in cursor:
            domain_activity[i['_id']] = i['count']

        for key, value in data.iteritems():
            activity = 'orangeflag'
            activity_yesterday = 'orangeflag'
            activity_before_yesterday = 'orangeflag'
            if value.get('impressions_block_2', 0) > 100:
                activity_yesterday = 'greenflag'
            if value.get('impressions_block_3', 0) > 100:
                activity_before_yesterday = 'greenflag'
            if value.get('impressions_block', 0) > 100:
                activity = 'greenflag'
            if (activity == 'orangeflag') and (
                        (activity_yesterday != 'orangeflag') or (activity_before_yesterday != 'orangeflag')):
                activity = 'redflag'
            data[key].update({
                'activity': activity,
                'activity_yesterday': activity_yesterday,
                'activity_before_yesterday': activity_yesterday,
                'active_domains': {'today': domain_activity[key],
                                   'yesterday': 0,
                                   'before_yesterday': 0}

            })

        operations = []

        for key, value in data.iteritems():
            try:
                operations.append(pymongo.UpdateOne({'user': key}, {'$set': value}, upsert=True))
            except Exception as ex:
                print(ex, "stats.user data", key, value)

        try:
            self.db.stats.user.summary.bulk_write(operations, ordered=False)
        except BulkWriteError as bwe:
            print(bwe.details)

        act_acc_count = 0
        domains_today = 0
        users = [x['login'] for x in self.db.users.find({'accountType': 'user'}).sort('registrationDate')]
        for x in self.db.stats.user.summary.find():
            if x['user'] not in users: continue
            if x['activity'] == 'greenflag':
                act_acc_count += 1
            domains_today += x.get('active_domains', {}).get('today', 0)

        self.db.stats.daily.all.update({'date': date},
                                       {'$set': {'act_acc_count': act_acc_count,
                                                 'domains_today': domains_today,
                                                 'acc_count': len(users)}},
                                       upsert=False)
        current_time = datetime.datetime.today()
        self.db.config.update({'key': 'last stats_user_summary update'},
                              {'$set': {'value': current_time}},
                              upsert=True)

    def createCatigoriesDomainReport(self, date):
        assert isinstance(date, (datetime.datetime, datetime.date))
        headers = {'X-Cache-Update': '1'}
        cdns = ['http://cdn.srv-10.yottos.com', 'http://cdn.srv-11.yottos.com',
                'http://cdn.srv-12.yottos.com', 'https://cdn.yottos.com']
        date = datetime.datetime(date.year, date.month, date.day, 0, 0)
        print(date)
        activ_domain = [item['domain'] for item in
                        self.db.stats.daily.domain.find({'date': date, 'impressions_block': {'$gte': 100}},
                                                        {'domain': 1, '_id': 0})]
        all_domain = []
        for item in self.db.domain.find({
            "domains": {
                "$exists": True
            },
            "login": {
                "$in": [item['login'] for item in self.db.users.find({"manager": False})]
            }
        }):
            for x in item.get('domains', {}).values():
                all_domain.append(x)
        category = {}
        cur = self.db.advertise.category.find()
        for item in cur:
            value = {'activ': [], 'notActiv': []}
            domain = self.db.domain.categories.find({"categories": item['guid']})
            for i in domain:
                if i['domain'] in activ_domain:
                    value['activ'].append(i['domain'])
                else:
                    value['notActiv'].append(i['domain'])
                if i['domain'] in all_domain:
                    all_domain.remove(i['domain'])
            category[item['title']] = value
        font0 = xlwt.Font()
        font0.name = 'Times New Roman'
        font0.colour_index = 0
        font0.height = 360
        font0.bold = True
        style0 = xlwt.XFStyle()
        style0.font = font0

        font1 = xlwt.Font()
        font1.name = 'Times New Roman'
        font1.colour_index = 0
        font1.height = 280
        font1.bold = False
        style1 = xlwt.XFStyle()
        style1.font = font1

        wbk = xlwt.Workbook('utf-8')
        sheet = wbk.add_sheet('Рубрикатор %s' % date.strftime('%Y-%m-%d'))
        sheet.write(0, 0, 'Категория', style0)
        sheet.write(0, 1, 'Активный', style0)
        sheet.write(0, 2, 'Не активный', style0)
        sheet.col(0).width = 256 * 50
        sheet.col(1).width = 256 * 50
        sheet.col(2).width = 256 * 50
        sheet.row(0).height_mismatch = True
        sheet.row(0).height = 400
        count = 1
        for key, value in category.iteritems():
            sheet.write(count, 0, key, style1)
            for idx, val in enumerate(value['activ']):
                sheet.write(count + idx, 1, val, style1)
                sheet.row(count + idx).height_mismatch = True
                sheet.row(count + idx).height = 300
            for idx, val in enumerate(value['notActiv']):
                sheet.write(count + idx, 2, val, style1)
                sheet.row(count + idx).height_mismatch = True
                sheet.row(count + idx).height = 300
            if len(value['activ']) >= len(value['notActiv']):
                count += len(value['activ'])
            else:
                count += len(value['notActiv'])
            sheet.write_merge(count + 1, count + 1, 0, 2, '', style1)
            count += 2
        sheet1 = wbk.add_sheet('Неназначеные')
        sheet1.col(0).width = 256 * 50
        for idx, val in enumerate(all_domain):
            sheet1.write(idx, 0, val, style1)
        buf = StringIO.StringIO()
        wbk.save(buf)
        buf.seek(0)
        ftp = ftplib.FTP(host='cdn.yottos.com')
        ftp.login('cdn', '$www-app$')
        ftp.cwd('report')
        ftp.storbinary('STOR category_report.xls', buf)
        ftp.close()
        for cdn in cdns:
            url = '%s/report/category_report.xls' % cdn
            r = requests.get(url, headers=headers, verify=False)
            print('%s - %s' % (url, r.status_code))
