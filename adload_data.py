# -*- coding: utf-8 -*-
import pymssql
import logging
import datetime
import uuid
import dateutil.parser

log = logging.getLogger(__name__)


def mssql_connection_adload():
    pymssql.set_max_connections(450)
    conn = pymssql.connect(host='srv-1.yottos.com',
                           user='web',
                           password='odif8duuisdofj',
                           database='1gb_YottosAdLoad',
                           as_dict=True,
                           charset='cp1251')
    conn.autocommit(True)
    return conn


class AdloadData(object):
    'Класс предоставляет интерфейс для взаимодействия и управления ``AdLoad``'

    def __init__(self, connection_adload):
        self.connection_adload = connection_adload

    def campaign_details(self, campaign):
        ''' Возвращает подробную информацию о кампании ``campaign``.
        Формат ответа::
            
            (struct)
                'id':      (string)
                'title':   (string)
                'getmyad': (bool)
            (struct-end)
        '''
        cursor = self.connection_adload.cursor()
        cursor.execute('''select a.AdvertiseId as AdvertiseID, a.UserID as UserID, Title, m.Name as Manager 
                        from Advertise a
                        left outer join Users u on u.UserID = a.UserID
                        left outer join Manager m  on u.ManagerID = m.id
                        where a.AdvertiseID = %s''', campaign)
        row = cursor.fetchone()
        if not row:
            cursor.close()
            return False
        cursor.close()
        return True

    def campaign_check(self, campaign):
        ''' Возвращает подробную информацию о кампании ``campaign``.
        Формат ответа::
        '''
        cursor = self.connection_adload.cursor()
        cursor.execute('''select isActive
                        from Advertise
                        where isActive = 1 and AdvertiseID = %s''', campaign)
        row = cursor.fetchone()
        if not row:
            cursor.close()
            return False
        cursor.close()
        return True
