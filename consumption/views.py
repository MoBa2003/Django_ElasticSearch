from django.shortcuts import render

from rest_framework.views import APIView
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from elasticsearch import Elasticsearch
from .serializers import ElectricityDataSerializer






es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])
INDEX_NAME = 'electricity_consumption'


class LowVoltageCountView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        
        query = {
            "query": {
                "range": {
                    "voltage": {
                        "lt": 220
                    }
                }
            }
        }

       
        response = es.count(index=INDEX_NAME, body=query)
        count = response['count']

        return Response({"voltage_below_220_count": count})
    

class ZeroGlobalActivePowerView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        query = {
            "query": {
                "term": {
                    "global_active_power": 0
                }
            }
        }
        response = es.count(index=INDEX_NAME, body=query)
        return Response({"zero_global_active_power_count": response['count']})


class ZeroSubMetering12View(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"sub_metering_1": 0}},
                        {"term": {"sub_metering_2": 0}}
                    ]
                }
            }
        }
        response = es.count(index=INDEX_NAME, body=query)
        return Response({"zero_sub_metering_1_and_2_count": response['count']})


class HighGlobalIntensityView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        query = {
            "query": {
                "range": {
                    "global_intensity": {
                        "gt": 20
                    }
                }
            }
        }
        response = es.count(index=INDEX_NAME, body=query)
        return Response({"global_intensity_above_20_count": response['count']})


class HighSubMetering2View(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        query = {
            "query": {
                "range": {
                    "sub_metering_2": {
                        "gt": 10
                    }
                }
            }
        }
        response = es.count(index=INDEX_NAME, body=query)
        return Response({"sub_metering_2_above_10_count": response['count']})


class HighVoltageView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        query = {
            "query": {
                "range": {
                    "voltage": {
                        "gt": 240
                    }
                }
            }
        }
        response = es.count(index=INDEX_NAME, body=query)
        return Response({"voltage_above_240_count": response['count']})


class ReactivePowerRangeView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        query = {
            "query": {
                "range": {
                    "global_reactive_power": {
                        "gte": 0.4,
                        "lte": 0.6
                    }
                }
            }
        }
        response = es.count(index=INDEX_NAME, body=query)
        return Response({"reactive_power_between_0.4_and_0.6_count": response['count']})


class HighActivePowerView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        query = {
            "query": {
                "range": {
                    "global_active_power": {
                        "gt": 5
                    }
                }
            }
        }
        response = es.search(index=INDEX_NAME, body=query)
        hits = [hit["_source"] for hit in response["hits"]["hits"]]
        return Response({"records_with_active_power_above_5": hits})


class SubMetering3Above10DaysView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
       
        query = {
            "size": 0,
            "query": {
                "range": {
                    "sub_metering_3": {
                        "gt": 10
                    }
                }
            },
            "aggs": {
                "days": {
                    "date_histogram": {
                        "field": "datetime",
                        "calendar_interval": "day"
                    }
                }
            }
        }
        response = es.search(index=INDEX_NAME, body=query)
        buckets = response['aggregations']['days']['buckets']
        days = [bucket['key_as_string'].split("T")[0] for bucket in buckets]
        return Response({"high_sub_metering_3_days": days})


class ZeroReactivePowerView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        query = {
            "query": {
                "term": {
                    "global_reactive_power": 0
                }
            }
        }
        response = es.count(index=INDEX_NAME, body=query)
        return Response({"zero_reactive_power_count": response['count']})







#################################################################################################

class AvgHighActivePowerView(APIView):
  
    permission_classes = [IsAuthenticated]

    def get(self, request):
        query = {
            "size": 0,
            "query": {
                "range": {
                    "global_active_power": {"gt": 5}
                }
            },
            "aggs": {
                "avg_power": {"avg": {"field": "global_active_power"}}
            }
        }
        res = es.search(index=INDEX_NAME, body=query)
        return Response({"avg_active_power_above_5": res["aggregations"]["avg_power"]["value"]})


class SubMetering1And2AboveZero(APIView):
  
    permission_classes = [IsAuthenticated]

    def get(self, request):
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"range": {"sub_metering_1": {"gt": 0}}},
                        {"range": {"sub_metering_2": {"gt": 0}}}
                    ]
                }
            }
        }
        res = es.count(index=INDEX_NAME, body=query)
        return Response({"sub_metering_1_and_2_above_zero": res["count"]})


class GlobalIntensityBetween22And25(APIView):
    
    permission_classes = [IsAuthenticated]

    def get(self, request):
        query = {
            "query": {
                "range": {
                    "global_intensity": {"gte": 22, "lte": 25}
                }
            }
        }
        res = es.count(index=INDEX_NAME, body=query)
        return Response({"global_intensity_22_to_25_count": res["count"]})


class AvgSubMetering1WhereSub3Above10(APIView):
   
    permission_classes = [IsAuthenticated]

    def get(self, request):
        query = {
            "size": 0,
            "query": {
                "range": {
                    "sub_metering_3": {"gt": 10}
                }
            },
            "aggs": {
                "avg_sub_1": {"avg": {"field": "sub_metering_1"}}
            }
        }
        res = es.search(index=INDEX_NAME, body=query)
        return Response({"avg_sub_metering_1_where_sub_3_gt_10": res["aggregations"]["avg_sub_1"]["value"]})


class HighActiveLowVoltageView(APIView):
   
    permission_classes = [IsAuthenticated]

    def get(self, request):
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"range": {"global_active_power": {"gt": 4}}},
                        {"range": {"voltage": {"lt": 230}}}
                    ]
                }
            }
        }
        res = es.count(index=INDEX_NAME, body=query)
        return Response({"high_power_low_voltage_count": res["count"]})


class AvgSub2At8AMView(APIView):
   
    permission_classes = [IsAuthenticated]

    def get(self, request):
       
        query = {
            "size": 0,
            "query": {
                "script": {
                    "script": {
                        "source": "doc['datetime'].value.getHour() == 8",
                        "lang": "painless"
                    }
                }
            },
            "aggs": {
                "avg_sub_2": {"avg": {"field": "sub_metering_2"}}
            }
        }
        res = es.search(index=INDEX_NAME, body=query)
        return Response({"avg_sub_metering_2_at_8am": res["aggregations"]["avg_sub_2"]["value"]})


class RecordsBetween18to22InDateView(APIView):
    
    permission_classes = [IsAuthenticated]

    def get(self, request):
       
        date_str = request.query_params.get('date', None)
        if not date_str:
            return Response({"error": "date parameter (yyyy-mm-dd) is required"}, status=400)

        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "datetime": {
                                    "gte": f"{date_str}T18:00:00",
                                    "lte": f"{date_str}T22:00:00"
                                }
                            }
                        }
                    ]
                }
            }
        }
        res = es.search(index=INDEX_NAME, body=query)
        hits = [h["_source"] for h in res["hits"]["hits"]]
        return Response({"records_between_18_22": hits})


class RecordsWhereAvgPowerAbove4(APIView):
    
    permission_classes = [IsAuthenticated]

    def get(self, request):
       
        query = {
            "size": 0,
            "aggs": {
                "avg_power": {"avg": {"field": "global_active_power"}}
            }
        }
        res = es.search(index=INDEX_NAME, body=query)
        avg = res["aggregations"]["avg_power"]["value"]
        return Response({"avg_global_active_power": avg, "above_4": avg > 4})


class AvgPowerWhereVoltageAbove233(APIView):
  
    permission_classes = [IsAuthenticated]

    def get(self, request):
        query = {
            "size": 0,
            "query": {
                "range": {
                    "voltage": {"gt": 233}
                }
            },
            "aggs": {
                "avg_power": {"avg": {"field": "global_active_power"}}
            }
        }
        res = es.search(index=INDEX_NAME, body=query)
        return Response({"avg_power_voltage_above_233": res["aggregations"]["avg_power"]["value"]})


class HighPowerAtNightView(APIView):
  
    permission_classes = [IsAuthenticated]

    def get(self, request):
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"range": {"global_active_power": {"gt": 5}}},
                        {
                            "script": {
                                "script": {
                                    "source": """
                                        def hour = doc['datetime'].value.getHour();
                                        return (hour >= 22 || hour < 6);
                                    """,
                                    "lang": "painless"
                                }
                            }
                        }
                    ]
                }
            }
        }
        res = es.count(index=INDEX_NAME, body=query)
        return Response({"night_high_power_count": res["count"]})
    

    
    
################################################################################################################

    

class DailyAvgActivePowerView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        query = {
            "size": 0,
            "aggs": {
                "daily_avg_power": {
                    "date_histogram": {
                        "field": "datetime",
                        "calendar_interval": "day"
                    },
                    "aggs": {
                        "avg_power": {"avg": {"field": "global_active_power"}}
                    }
                }
            }
        }
        res = es.search(index=INDEX_NAME, body=query)
        data = [
            {"date": b["key_as_string"], "avg_power": b["avg_power"]["value"]}
            for b in res["aggregations"]["daily_avg_power"]["buckets"]
        ]
        return Response(data)

class DailyAvgReactivePowerInRangeView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        start = request.query_params.get('start')
        end = request.query_params.get('end')
        if not start or not end:
            return Response({"error": "start and end parameters required"}, status=400)

        query = {
            "query": {
                "range": {
                    "datetime": {
                        "gte": start,
                        "lte": end
                    }
                }
            },
            "size": 0,
            "aggs": {
                "daily": {
                    "date_histogram": {
                        "field": "datetime",
                        "calendar_interval": "day"
                    },
                    "aggs": {
                        "avg_reactive": {"avg": {"field": "global_reactive_power"}}
                    }
                }
            }
        }
        res = es.search(index=INDEX_NAME, body=query)
        data = [
            {"date": b["key_as_string"], "avg_reactive_power": b["avg_reactive"]["value"]}
            for b in res["aggregations"]["daily"]["buckets"]
        ]
        return Response(data)

class HighSubMetering1DaysView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        query = {
            "size": 0,
            "aggs": {
                "days": {
                    "date_histogram": {
                        "field": "datetime",
                        "calendar_interval": "day"
                    },
                    "aggs": {
                        "avg_sub1": {"avg": {"field": "sub_metering_1"}}
                    }
                }
            }
        }
        res = es.search(index=INDEX_NAME, body=query)
        data = [
            {"date": b["key_as_string"], "avg_sub1": b["avg_sub1"]["value"]}
            for b in res["aggregations"]["days"]["buckets"] if b["avg_sub1"]["value"] and b["avg_sub1"]["value"] > 5
        ]
        return Response(data)

class DailyVoltageDiffView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        query = {
            "size": 0,
            "aggs": {
                "days": {
                    "date_histogram": {
                        "field": "datetime",
                        "calendar_interval": "day"
                    },
                    "aggs": {
                        "max_voltage": {"max": {"field": "voltage"}},
                        "min_voltage": {"min": {"field": "voltage"}}
                    }
                }
            }
        }
        res = es.search(index=INDEX_NAME, body=query)
        data = [
            {
                "date": b["key_as_string"],
                "voltage_diff": b["max_voltage"]["value"] - b["min_voltage"]["value"]
            }
            for b in res["aggregations"]["days"]["buckets"]
        ]
        return Response(data)

class WeeklySumSubMetering3View(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        query = {
            "size": 0,
            "aggs": {
                "weekly": {
                    "date_histogram": {
                        "field": "datetime",
                        "calendar_interval": "day"
                    },
                    "aggs": {
                        "sum_sub3": {"sum": {"field": "sub_metering_3"}}
                    }
                }
            }
        }
        res = es.search(index=INDEX_NAME, body=query)
        days = res['aggregations']['weekly']['buckets'][:7]
        total = sum([d['sum_sub3']['value'] for d in days])
        return Response({"total_sub_metering_3_week1": total})

class DailyPeakCountView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        query = {
            "size": 0,
            "query": {
                "range": {
                    "global_active_power": {"gt": 4}
                }
            },
            "aggs": {
                "daily": {
                    "date_histogram": {
                        "field": "datetime",
                        "calendar_interval": "day"
                    }
                }
            }
        }
        res = es.search(index=INDEX_NAME, body=query)
        data = [
            {"date": b["key_as_string"], "count": b["doc_count"]}
            for b in res["aggregations"]["daily"]["buckets"]
        ]
        return Response(data)

class HourlyAvgIntensityView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        query = {
            "size": 0,
            "aggs": {
                "by_hour": {
                    "date_histogram": {
                        "field": "datetime",
                        "calendar_interval": "hour"
                    },
                    "aggs": {
                        "avg_intensity": {"avg": {"field": "global_intensity"}}
                    }
                }
            }
        }
        res = es.search(index=INDEX_NAME, body=query)
        data = [
            {"hour": b["key_as_string"], "avg_intensity": b["avg_intensity"]["value"]}
            for b in res["aggregations"]["by_hour"]["buckets"]
        ]
        return Response(data)

class DailySub2RatioView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        query = {
            "size": 0,
            "aggs": {
                "daily": {
                    "date_histogram": {
                        "field": "datetime",
                        "calendar_interval": "day"
                    },
                    "aggs": {
                        "sub2": {"sum": {"field": "sub_metering_2"}},
                        "active": {"sum": {"field": "global_active_power"}}
                    }
                }
            }
        }
        res = es.search(index=INDEX_NAME, body=query)
        data = []
        for b in res["aggregations"]["daily"]["buckets"]:
            sub2 = b["sub2"]["value"]
            active = b["active"]["value"]
            ratio = (sub2 / active) if active else None
            data.append({"date": b["key_as_string"], "ratio": ratio})
        return Response(data)

class Top5AvgActivePowerDaysView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        query = {
            "size": 0,
            "aggs": {
                "daily": {
                    "date_histogram": {
                        "field": "datetime",
                        "calendar_interval": "day"
                    },
                    "aggs": {
                        "avg_power": {"avg": {"field": "global_active_power"}}
                    }
                }
            }
        }
        res = es.search(index=INDEX_NAME, body=query)
        buckets = res["aggregations"]["daily"]["buckets"]
        sorted_buckets = sorted(buckets, key=lambda b: b["avg_power"]["value"] or 0, reverse=True)
        top5 = sorted_buckets[:5]
        data = [
            {"date": b["key_as_string"], "avg_power": b["avg_power"]["value"]}
            for b in top5
        ]
        return Response(data)

class DaysSubMeteringExceedsPowerView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        query = {
            "size": 0,
            "aggs": {
                "daily": {
                    "date_histogram": {
                        "field": "datetime",
                        "calendar_interval": "day"
                    },
                    "aggs": {
                        "sub1": {"sum": {"field": "sub_metering_1"}},
                        "sub2": {"sum": {"field": "sub_metering_2"}},
                        "sub3": {"sum": {"field": "sub_metering_3"}},
                        "active": {"sum": {"field": "global_active_power"}}
                    }
                }
            }
        }
        res = es.search(index=INDEX_NAME, body=query)
        result = []
        for b in res["aggregations"]["daily"]["buckets"]:
            sub_total = b["sub1"]["value"] + b["sub2"]["value"] + b["sub3"]["value"]
            active = b["active"]["value"]
            if sub_total > active:
                result.append({"date": b["key_as_string"], "sub_total": sub_total, "active": active})
        return Response(result)

