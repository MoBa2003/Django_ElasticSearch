from django.db import models



class ElectricityData(models.Model):
    datetime = models.DateTimeField()
    global_active_power = models.FloatField(null=True)
    global_reactive_power = models.FloatField(null=True)
    voltage = models.FloatField(null=True)
    global_intensity = models.FloatField(null=True)
    sub_metering_1 = models.FloatField(null=True)
    sub_metering_2 = models.FloatField(null=True)
    sub_metering_3 = models.FloatField(null=True)

    class Meta:
        managed = False  # چون داده‌ها از الستیک سرچ میان
