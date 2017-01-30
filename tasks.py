# -*- coding: utf-8 -*-
import datetime
import xmlrpclib

from celery.task import periodic_task
from celery.schedules import crontab
import pymongo
from statistic import GetmyadStats
from manager import GetmyadManagerStats
from clean import GetmyadClean
from check import GetmyadCheck
from rating import GetmyadRating

GETMYAD_XMLRPC_HOST = 'https://getmyad.yottos.com/rpc'
MONGO_HOST = 'srv-5.yottos.com:27018,srv-9.yottos.com:27018,srv-5.yottos.com:27019,srv-8.yottos.com:27018'
MONGO_DATABASE = 'getmyad_db'
MONGO_WORKER_HOST_POOL = ['srv-3.yottos.com:27017', 'srv-6.yottos.com:27017', 'srv-7.yottos.com:27017',
                          'srv-8.yottos.com:27017', 'srv-9.yottos.com:27017']
MONGO_WORKER_DATABASE = 'getmyad'


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
    u"""Возвращает подключение к базе данных MongoDB"""
    return _mongo_connection(MONGO_HOST)[MONGO_DATABASE]


def _mongo_worker_db_pool():
    u"""Возвращает подключение к базе данных MongoDB"""
    pool = []
    for host in MONGO_WORKER_HOST_POOL:
        try:
            pool.append(_mongo_connection(host)[MONGO_WORKER_DATABASE])
        except Exception as e:
            print e, host
    return pool


def test():
    u"""Обработка (агрегация) статистики"""
    db = _mongo_main_db()
    pool = _mongo_worker_db_pool()
    elapsed_start_time = datetime.datetime.now()
    GetmyadStats().importWorkerBlockData(db, pool)
    GetmyadStats().importWorkerOfferData(db, pool)
    # GetmyadStats().importClicksFromMongo(db)


@periodic_task(run_every=crontab(hour="*", minute=0))
def clean_ip_blacklist():
    u"""Удаляет старые записи из чёрного списка"""
    print 'Clean IP Blacklist is start'
    elapsed_start_time = datetime.datetime.now()
    db = _mongo_main_db()
    GetmyadClean().clean_ip_blacklist(db)
    print 'Clean IP Blacklist is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds


@periodic_task(run_every=crontab(hour=[1, 3, 6, 9, 12, 15, 18, 21, 23], minute=0))
def delete_old_offers():
    u"""Удаляет старые записи из чёрного списка"""
    print 'Delete old offers is start'
    elapsed_start_time = datetime.datetime.now()
    db = _mongo_main_db()
    rpc = xmlrpclib.ServerProxy(GETMYAD_XMLRPC_HOST)
    GetmyadClean().delete_old_offers(db, rpc)
    print 'Delete old offers is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds


@periodic_task(run_every=crontab(hour=[1, 11, 16, 21], minute=0))
def managerInvoceCalck():
    print 'Manager invoce calck stats is start'
    elapsed_start_time = datetime.datetime.now()
    db = _mongo_main_db()
    GetmyadManagerStats().culculateInvoce(db, datetime.date.today())
    print 'Manager invoce calck stats is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds
    pass


@periodic_task(run_every=crontab(hour=[0, 8, 16], minute=0))
def decline_unconfirmed_moneyout_requests():
    u"""Отклоняет заявки, которые пользователи не подтвердили в течении трёх
        дней"""
    print 'Decline unconfirmed moneyout requests is start'
    elapsed_start_time = datetime.datetime.now()
    db = _mongo_main_db()
    GetmyadClean().decline_unconfirmed_moneyout_requests(db)
    print 'Decline unconfirmed moneyout requests is end %s second' % (
        datetime.datetime.now() - elapsed_start_time).seconds


@periodic_task(run_every=crontab(minute="20, 45", hour="*"))
def createOfferRating():
    u"""Создаем отдельные рейтинги для каждого рекламного блока"""
    elapsed_start_time = datetime.datetime.now()
    db = _mongo_main_db()
    pool = _mongo_worker_db_pool()
    print 'Import worker rating data to stats_daily is start'
    GetmyadRating().importWorkerData(db, pool)
    print 'Import worker rating data elapsed %s second' % (datetime.datetime.now() - elapsed_start_time).seconds
    print 'Count clicks to rating is start'
    GetmyadRating().importClicksFromMongo(db)
    print 'Count cliks elapsed %s second' % (datetime.datetime.now() - elapsed_start_time).seconds
    print 'Create rating for offer is start'
    GetmyadRating().createOfferRating(db)
    GetmyadRating().createOfferRadingForInformers(db)
    print 'Create rating for offer is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds


@periodic_task(run_every=crontab(minute="0", hour="0"))
def delete_old_stats():
    u"""Удаляем старую статистику"""
    print 'Delete old data is start'
    elapsed_start_time = datetime.datetime.now()
    db = _mongo_main_db()
    GetmyadClean().delete_old_stats(db)
    print 'Delete old data is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds


@periodic_task(run_every=crontab(minute="0", hour="0"))
def delete_click_rejected():
    u"""Удаляем старые отклонённые клики"""
    print 'Delete old click rejected is start'
    elapsed_start_time = datetime.datetime.now()
    db = _mongo_main_db()
    GetmyadClean().delete_click_rejected(db)
    print 'Delete old click rejected is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds


@periodic_task(run_every=crontab(minute=10, hour=0))
def delete_old_rating_stats():
    u"""Удаляем старую статистику для рейтингов"""
    print 'Delete old rating data is start'
    elapsed_start_time = datetime.datetime.now()
    db = _mongo_main_db()
    GetmyadRating().trunkete_rating_stats(db)
    GetmyadRating().delete_old_rating_stats(db)
    print 'Delete old rating data is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds


@periodic_task(run_every=crontab(minute="12", hour="*"))
def check_outdated_campaigns():
    u"""Иногда AdLoad не оповещает GetMyAd об остановке кампании.
        Данная задача проверяет, не произошло ли за последнее время несколько
        таких ошибок и, если произошло, обновляет кампанию."""
    print 'Check outdate campaigns is start'
    elapsed_start_time = datetime.datetime.now()
    db = _mongo_main_db()
    rpc = xmlrpclib.ServerProxy(GETMYAD_XMLRPC_HOST)
    GetmyadCheck().check_outdated_campaigns(db, rpc)
    print 'Check outdate campaigns is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds


@periodic_task(run_every=crontab(minute="0", hour="2,8,19"))
def check_campaigns():
    u"""Иногда AdLoad не оповещает GetMyAd об остановке кампании.
        Данная задача проверяет, не произошло ли за последнее время несколько
        таких ошибок и, если произошло, обновляет кампанию."""
    print 'Check outdate campaigns is start'
    elapsed_start_time = datetime.datetime.now()
    db = _mongo_main_db()
    rpc = xmlrpclib.ServerProxy(GETMYAD_XMLRPC_HOST)
    GetmyadCheck().check_campaigns(db, rpc)
    print 'Check outdate campaigns is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds


@periodic_task(run_every=crontab(hour="*", minute="15, 40"))
def stats_daily_adv_update():
    u"""Обработка (агрегация) статистики"""
    db = _mongo_main_db()
    pool = _mongo_worker_db_pool()
    # За сегодня
    elapsed_start_time = datetime.datetime.now()
    print 'Import worker data to stats_daily is start'
    GetmyadStats().importWorkerBlockData(db, pool)
    GetmyadStats().importWorkerOfferData(db, pool)
    GetmyadStats().importWorkerData(db, pool)
    print 'Import worker data elapsed %s second' % (datetime.datetime.now() - elapsed_start_time).seconds
    print 'Count clicks to stats_daily is start'
    GetmyadStats().importClicksFromMongo(db)
    print 'Count cliks elapsed %s second' % (datetime.datetime.now() - elapsed_start_time).seconds
    print 'Update stats_daily_adv is start'
    GetmyadStats().processMongoStats(db, datetime.date.today())
    print 'Update stats_daily_adv elapsed %s second' % (datetime.datetime.now() - elapsed_start_time).seconds
    print 'Agregate stats Domain is start'
    elapsed_start_time = datetime.datetime.now()
    GetmyadStats().agregateStatDailyDomain(db, datetime.date.today())
    print 'Agregate stats Domain is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds
    print 'Agregate User stats is start'
    elapsed_start_time = datetime.datetime.now()
    GetmyadStats().agregateStatDailyUser(db, datetime.date.today())
    print 'Agregate User stats is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds
    print 'Agregate Daily stats is start'
    elapsed_start_time = datetime.datetime.now()
    GetmyadStats().agregateStatDailyAll(db, datetime.date.today())
    print 'Agregate Daily stats is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds
    print 'Agregate UserSumary stats is start'
    elapsed_start_time = datetime.datetime.now()
    GetmyadStats().agregateStatUserSummary(db, datetime.date.today())
    print 'Agregate UserSumary stats is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds


@periodic_task(run_every=crontab(hour="5", minute="30"))
def stats_daily_adv_update_tomoroy():
    u"""Обработка (агрегация) статистики"""
    db = _mongo_main_db()
    # За вчера
    print 'Update stats_daily_adv is start'
    elapsed_start_time = datetime.datetime.now()
    GetmyadStats().processMongoStats(db, (datetime.date.today() - datetime.timedelta(days=1)))
    print 'Update stats_daily_adv elapsed %s second' % (datetime.datetime.now() - elapsed_start_time).seconds
    print 'Agregate stats Domain is start'
    elapsed_start_time = datetime.datetime.now()
    GetmyadStats().agregateStatDailyDomain(db, (datetime.date.today() - datetime.timedelta(days=1)))
    print 'Agregate stats Domain is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds
    print 'Agregate User stats is start'
    elapsed_start_time = datetime.datetime.now()
    GetmyadStats().agregateStatDailyUser(db, (datetime.date.today() - datetime.timedelta(days=1)))
    print 'Agregate User stats is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds
    print 'Agregate Daily stats is start'
    elapsed_start_time = datetime.datetime.now()
    GetmyadStats().agregateStatDailyAll(db, (datetime.date.today() - datetime.timedelta(days=1)))
    print 'Agregate Daily stats is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds


@periodic_task(run_every=crontab(minute="0", hour="23"))
def create_xsl_report():
    u"""Создаёт xls отчёты"""
    print 'Create XLS report'
    elapsed_start_time = datetime.datetime.now()
    db = _mongo_main_db()
    GetmyadStats().createCatigoriesDomainReport(db, (datetime.date.today() - datetime.timedelta(days=1)))
    print 'Create XLS report is end %s second' % (datetime.datetime.now() - elapsed_start_time).seconds
