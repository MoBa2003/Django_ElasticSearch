from django.urls import path
from . import views
from django.urls import path
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView

urlpatterns = [
   
    path('api/token/', TokenObtainPairView.as_view(), name='token_obtain_pair'),  
    path('api/token/refresh/', TokenRefreshView.as_view(), name='token_refresh'),  
    path('api/low-voltage/', views.LowVoltageCountView.as_view(), name='low-voltage'),
    path('api/zero-active-power/', views.ZeroGlobalActivePowerView.as_view(), name='zero-active-power'),
    path('api/zero-sub12/', views.ZeroSubMetering12View.as_view(), name='zero-sub12'),
    path('api/high-intensity/', views.HighGlobalIntensityView.as_view(), name='high-intensity'),
    path('api/high-sub2/', views.HighSubMetering2View.as_view(), name='high-sub2'),
    path('api/high-voltage/', views.HighVoltageView.as_view(), name='high-voltage'),
    path('api/reactive-range/', views.ReactivePowerRangeView.as_view(), name='reactive-range'),
    path('api/high-active-power/', views.HighActivePowerView.as_view(), name='high-active-power'),
    path('api/high-sub3-days/', views.SubMetering3Above10DaysView.as_view(), name='high-sub3-days'),
    path('api/zero-reactive/', views.ZeroReactivePowerView.as_view(), name='zero-reactive'),
    
    
    path('avg-high-active-power/', views.AvgHighActivePowerView.as_view()),
    path('sub-metering-1-2-above-zero/', views.SubMetering1And2AboveZero.as_view()),
    path('global-intensity-between-22-25/', views.GlobalIntensityBetween22And25.as_view()),
    path('avg-sub1-when-sub3-above-10/', views.AvgSubMetering1WhereSub3Above10.as_view()),
    path('high-power-low-voltage/', views.HighActiveLowVoltageView.as_view()),
    path('avg-sub2-at-8am/', views.AvgSub2At8AMView.as_view()),
    path('records-between-18-22/', views.RecordsBetween18to22InDateView.as_view()),  
    path('check-avg-power-above-4/', views.RecordsWhereAvgPowerAbove4.as_view()),
    path('avg-power-voltage-above-233/', views.AvgPowerWhereVoltageAbove233.as_view()),
    path('night-high-power/', views.HighPowerAtNightView.as_view()),
    
    
    
    path('daily-avg-active-power/', views.DailyAvgActivePowerView.as_view()),
    path('daily-avg-reactive-power/', views.DailyAvgReactivePowerInRangeView.as_view()),
    path('high-sub1-days/', views.HighSubMetering1DaysView.as_view()),
    path('daily-voltage-diff/', views.DailyVoltageDiffView.as_view()),
    path('weekly-sum-sub3/', views.WeeklySumSubMetering3View.as_view()),
    path('daily-peak-count/', views.DailyPeakCountView.as_view()),
    path('hourly-avg-intensity/', views.HourlyAvgIntensityView.as_view()),
    path('daily-sub2-ratio/', views.DailySub2RatioView.as_view()),
    path('top5-avg-active-days/', views.Top5AvgActivePowerDaysView.as_view()),
    path('days-submetering-exceeds-power/', views.DaysSubMeteringExceedsPowerView.as_view()),
    
    
    
]
