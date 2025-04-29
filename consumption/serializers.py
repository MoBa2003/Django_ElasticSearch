
from rest_framework import serializers
from .models import ElectricityData

class ElectricityDataSerializer(serializers.Serializer):
    datetime = serializers.DateTimeField()
    global_active_power = serializers.FloatField()
    global_reactive_power = serializers.FloatField()
    voltage = serializers.FloatField()
    global_intensity = serializers.FloatField()
    sub_metering_1 = serializers.FloatField()
    sub_metering_2 = serializers.FloatField()
    sub_metering_3 = serializers.FloatField()
