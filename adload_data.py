# -*- coding: utf-8 -*-
import logging

import pymssql

log = logging.getLogger(__name__)


def mssql_connection_adload():
    pymssql.set_max_connections(450)
    conn = pymssql.connect(host='srv-3.yottos.com',
                           user='web',
                           password='odif8duuisdofj',
                           database='Adload',
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
        with self.connection_adload.cursor(as_dict=True) as cursor:
            cursor.execute('''SELECT 1 AS status
                            FROM View_Advertise AS a
                            LEFT OUTER JOIN Users AS u ON u.UserID = a.UserID
                            LEFT OUTER JOIN Manager AS m  ON u.ManagerID = m.id
                            WHERE  a.AdvertiseID = %s''', campaign)
            if cursor.fetchone() is None:
                return False
            return True

    def campaign_check(self, campaign):
        ''' Возвращает подробную информацию о кампании ``campaign``.
        Формат ответа::
        '''
        with self.connection_adload.cursor(as_dict=True) as cursor:
            cursor.execute('''SELECT TOP 1 1 AS status FROM View_Lot AS l
                              INNER JOIN LotByAdvertise AS la ON l.LotID = la.LotID
                              INNER JOIN View_Advertise as va on la.AdvertiseID = va.AdvertiseID
                              WHERE la.AdvertiseID = %s AND l.isAdvertising = 1 AND and va.isActive = 1''', campaign)
            if cursor.fetchone() is None:
                return False
            return True
