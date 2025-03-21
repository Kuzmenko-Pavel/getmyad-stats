# -*- coding: utf-8 -*-
import datetime
import xmlrpclib

import pymongo
from celery.schedules import crontab
from celery.task import periodic_task

from check import GetmyadCheck
from clean import GetmyadClean
from manager import GetmyadManagerStats
from rating import GetmyadRating
from statistic import GetmyadStats

GETMYAD_XMLRPC_HOST = 'https://getmyad.yottos.com/rpc'
MONGO_HOST = 'srv-3.yottos.com:27017'
MONGO_DATABASE = 'getmyad_db'
MONGO_WORKER_HOST_POOL = ['srv-2.yottos.com:27017', ]


def _mongo_connection(host):
    u"""Возвращает Connection к серверу MongoDB"""
    try:
        connection = pymongo.MongoClient(host=host)
    except pymongo.errors.AutoReconnect:
        # Пауза и повторная попытка подключиться
        from time import sleep
        sleep(1)
        connection = pymongo.MongoClient(host=host)
    return connection


def _mongo_main_db():
    """Возвращает подключение к базе данных MongoDB"""
    return _mongo_connection(MONGO_HOST)[MONGO_DATABASE]


def _mongo_worker_db_pool():
    """Возвращает подключение к базе данных MongoDB Worker"""
    pool = []
    # now = datetime.datetime.now()
    # first_db = 'rg_%s' % now.hour
    # second_db = 'rg_%s' % (now - datetime.timedelta(minutes=60)).hour
    # mongo_worker_database = list([first_db, second_db])
    # mongo_worker_database.append('getmyad')
    mongo_worker_database = list(['getmyad_log', ])
    for host in MONGO_WORKER_HOST_POOL:
        try:
            for base_name in mongo_worker_database:
                pool.append(_mongo_connection(host)[base_name])
        except Exception as e:
            print(e, host)
    return pool


# @periodic_task(run_every=crontab(hour="*", minute=0))
def clean_ip_blacklist():
    u"""Удаляет старые записи из чёрного списка"""
    print('Clean IP Blacklist is start')
    elapsed_start_time = datetime.datetime.now()
    db = _mongo_main_db()
    clean = GetmyadClean(db)
    clean.clean_ip_blacklist()
    print('Clean IP Blacklist is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds)


#@periodic_task(run_every=crontab(hour=[1, 3, 6, 9, 12, 15, 18, 21, 23], minute=0))
def delete_old_offers():
    u"""Удаляет старые записи из чёрного списка"""
    print('Delete old offers is start')
    elapsed_start_time = datetime.datetime.now()
    db = _mongo_main_db()
    clean = GetmyadClean(db)
    clean.delete_old_offers()
    print('Delete old offers is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds)


#@periodic_task(run_every=crontab(hour=[1, 11, 16, 21], minute=0))
def manager_invoce_calck():
    print('Manager invoce calck stats is start')
    elapsed_start_time = datetime.datetime.now()
    db = _mongo_main_db()
    GetmyadManagerStats().culculateInvoce(db, datetime.date.today())
    print('Manager invoce calck stats is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds)


#@periodic_task(run_every=crontab(hour=[0, 8, 16], minute=0))
def decline_unconfirmed_moneyout_requests():
    u"""Отклоняет заявки, которые пользователи не подтвердили в течении трёх
        дней"""
    print('Decline unconfirmed moneyout requests is start')
    elapsed_start_time = datetime.datetime.now()
    db = _mongo_main_db()
    clean = GetmyadClean(db)
    clean.decline_unconfirmed_moneyout_requests()
    print('Decline unconfirmed moneyout requests is end %s second' % (
        datetime.datetime.now() - elapsed_start_time).seconds)


#@periodic_task(run_every=crontab(minute="20, 45", hour="*"))
def create_offer_rating():
    u"""Создаем отдельные рейтинги для каждого рекламного блока"""
    elapsed_start_time = datetime.datetime.now()
    db = _mongo_main_db()
    pool = _mongo_worker_db_pool()
    rating = GetmyadRating(db, pool)
    print('Import worker rating data to stats_daily is start')
    rating.importWorkerData()
    print('Import worker rating data elapsed %s second' % (datetime.datetime.now() - elapsed_start_time).seconds)
    print('Count clicks to rating is start')
    rating.importClicksFromMongo()
    print('Count cliks elapsed %s second' % (datetime.datetime.now() - elapsed_start_time).seconds)
    print('Create rating for offer is start')
    rating.createOfferRating()
    rating.createCampaignRatingForInformers()
    rating.createOfferRatingForInformers()
    print('Create rating for offer is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds)


#@periodic_task(run_every=crontab(minute="0", hour="0"))
def stop_old_campaign():
    u"""Останавливаем компании из холда"""
    print('Stop old campaign is start')
    elapsed_start_time = datetime.datetime.now()
    db = _mongo_main_db()
    clean = GetmyadClean(db)
    rpc = xmlrpclib.ServerProxy(GETMYAD_XMLRPC_HOST)
    clean.stop_old_campaign(rpc)
    print('Stop old campaign is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds)


#@periodic_task(run_every=crontab(minute="0", hour="0"))
def campaign_thematic():
    u"""Останавливаем компании из холда"""
    print('Start update thematic campaign')
    elapsed_start_time = datetime.datetime.now()
    db = _mongo_main_db()
    rpc = xmlrpclib.ServerProxy(GETMYAD_XMLRPC_HOST)
    check = GetmyadCheck(db, rpc)
    check.campaign_thematic()
    print('Stop update thematic campaign is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds)


#@periodic_task(run_every=crontab(minute="0", hour="0"))
def delete_old_stats():
    u"""Удаляем старую статистику"""
    print('Delete old data is start')
    elapsed_start_time = datetime.datetime.now()
    db = _mongo_main_db()
    clean = GetmyadClean(db)
    clean.delete_old_stats()
    print('Delete old data is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds)


#@periodic_task(run_every=crontab(minute="0", hour="0"))
def delete_click_rejected():
    u"""Удаляем старые отклонённые клики"""
    print('Delete old click rejected is start')
    elapsed_start_time = datetime.datetime.now()
    db = _mongo_main_db()
    clean = GetmyadClean(db)
    clean.delete_click_rejected()
    print('Delete old click rejected is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds)


#@periodic_task(run_every=crontab(minute=10, hour=0))
def delete_old_rating_stats():
    u"""Удаляем старую статистику для рейтингов"""
    print('Delete old rating data is start')
    elapsed_start_time = datetime.datetime.now()
    db = _mongo_main_db()
    pool = _mongo_worker_db_pool()
    rating = GetmyadRating(db, pool)
    rating.delete_old_rating_stats()
    rating.trunkete_rating_stats()
    print('Delete old rating data is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds)


#@periodic_task(run_every=crontab(minute=[5, 15, 25, 35, 45, 55]))
def check_outdated_campaigns():
    u"""Иногда AdLoad не оповещает GetMyAd об остановке кампании.
        Данная задача проверяет, не произошло ли за последнее время несколько
        таких ошибок и, если произошло, обновляет кампанию."""
    print('Check outdate campaigns is start')
    elapsed_start_time = datetime.datetime.now()
    db = _mongo_main_db()
    rpc = xmlrpclib.ServerProxy(GETMYAD_XMLRPC_HOST)
    check = GetmyadCheck(db, rpc)
    check.check_outdated_campaigns()
    print('Check outdate campaigns is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds)


#@periodic_task(run_every=crontab(minute=[0, 10, 20, 30, 40, 50]))
def check_campaigns():
    u"""Иногда AdLoad не оповещает GetMyAd об остановке кампании.
        Данная задача проверяет, не произошло ли за последнее время несколько
        таких ошибок и, если произошло, обновляет кампанию."""
    print('Check outdate campaigns is start')
    elapsed_start_time = datetime.datetime.now()
    db = _mongo_main_db()
    rpc = xmlrpclib.ServerProxy(GETMYAD_XMLRPC_HOST)
    check = GetmyadCheck(db, rpc)
    check.check_campaigns()
    print('Check outdate campaigns is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds)


#@periodic_task(run_every=crontab(minute=[0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]))
def check_cdn():
    print('Check cdn is start')
    elapsed_start_time = datetime.datetime.now()
    db = _mongo_main_db()
    rpc = xmlrpclib.ServerProxy(GETMYAD_XMLRPC_HOST)
    check = GetmyadCheck(db, rpc)
    check.check_cdn()
    print('Check cdn is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds)


@periodic_task(run_every=crontab(hour="*", minute="0"))
def stats_daily_adv_update():
    u"""Обработка (агрегация) статистики"""
    db = _mongo_main_db()
    pool = _mongo_worker_db_pool()
    stats = GetmyadStats(db, pool)
    # За сегодня
    elapsed_start_time = datetime.datetime.now()
    print('Import worker data to stats_daily is start')
    stats.import_retargeting_track_data()
    stats.importWorkerBlockData()
    stats.importWorkerOfferData()
    print('Import worker data elapsed %s second' % (datetime.datetime.now() - elapsed_start_time).seconds)
    print('Count clicks to stats_daily is start')
    stats.importClicksFromMongo()
    stats.importBlockClicksFromMongo()
    print('Count cliks elapsed %s second' % (datetime.datetime.now() - elapsed_start_time).seconds)
    print('Update stats_daily_adv is start')
    stats.processMongoStats(datetime.date.today())
    print('Update stats_daily_adv elapsed %s second' % (datetime.datetime.now() - elapsed_start_time).seconds)
    print('Agregate stats Domain is start')
    elapsed_start_time = datetime.datetime.now()
    stats.agregateStatDailyDomain(datetime.date.today())
    print('Agregate stats Domain is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds)
    print('Agregate User stats is start')
    elapsed_start_time = datetime.datetime.now()
    stats.agregateStatDailyUser(datetime.date.today())
    print('Agregate User stats is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds)
    print('Agregate Daily stats is start')
    elapsed_start_time = datetime.datetime.now()
    stats.agregateStatDailyAll(datetime.date.today())
    print('Agregate Daily stats is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds)
    print('Agregate UserSumary stats is start')
    elapsed_start_time = datetime.datetime.now()
    stats.agregateStatUserSummary(datetime.date.today())
    print('Agregate UserSumary stats is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds)


@periodic_task(run_every=crontab(hour="5", minute="30"))
def stats_daily_adv_update_tomoroy():
    u"""Обработка (агрегация) статистики"""
    db = _mongo_main_db()
    pool = _mongo_worker_db_pool()
    stats = GetmyadStats(db, pool)
    # За вчера
    print('Update stats_daily_adv is start')
    elapsed_start_time = datetime.datetime.now()
    stats.processMongoStats(datetime.date.today() - datetime.timedelta(days=1))
    print('Update stats_daily_adv elapsed %s second' % (datetime.datetime.now() - elapsed_start_time).seconds)
    print('Agregate stats Domain is start')
    elapsed_start_time = datetime.datetime.now()
    stats.agregateStatDailyDomain(datetime.date.today() - datetime.timedelta(days=1))
    print('Agregate stats Domain is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds)
    print('Agregate User stats is start')
    elapsed_start_time = datetime.datetime.now()
    stats.agregateStatDailyUser(datetime.date.today() - datetime.timedelta(days=1))
    print('Agregate User stats is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds)
    print('Agregate Daily stats is start')
    elapsed_start_time = datetime.datetime.now()
    stats.agregateStatDailyAll(datetime.date.today() - datetime.timedelta(days=1))
    print('Agregate Daily stats is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds)


# @periodic_task(run_every=crontab(minute="0", hour="23"))
def create_xsl_report():
    u"""Создаёт xls отчёты"""
    print('Create XLS report')
    elapsed_start_time = datetime.datetime.now()
    db = _mongo_main_db()
    pool = _mongo_worker_db_pool()
    stats = GetmyadStats(db, pool)
    stats.createCatigoriesDomainReport(datetime.date.today() - datetime.timedelta(days=1))
    print('Create XLS report is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds)
