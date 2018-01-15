# -*- coding: utf-8 -*-
import datetime


class GetmyadManagerStats(object):
    def culculateInvoce(self, db, date):
        date = datetime.datetime(date.year, date.month, date.day, 0, 0)
        for manager in db.users.find({"accountType": "manager"}):
            users = [x['login'] for x in db.users.find({'managerGet': manager['login']})]
            costs = db.stats.daily.user.group(['date'],
                                              {'user': {'$in': users},
                                               'date': {'$gte': date - datetime.timedelta(days=1), '$lte': date}},
                                              {'adload_cost': 0, 'count': 0, 'income': 0, 'totalCost': 0},
                                              '''function(o,p) {
                                              p.adload_cost += o.adload_cost || 0;
                                              p.income += o.income || 0;
                                              p.totalCost += o.totalCost || 0;
                                              p.count +=1}''')
            for cost in costs:
                db.stats_manager_overall_by_date.update(
                    {"date": cost["date"], "login": manager["login"]},
                    {'$set': {
                        "adload_cost": cost["adload_cost"],
                        "income": cost["income"],
                        "totalCost": cost["totalCost"],
                        "activ_users": cost["count"],
                        "all_users": len(users)
                    }
                    }, True)
