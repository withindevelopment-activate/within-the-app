from django.urls import path
from . import views

app_name = 'Demo'
urlpatterns = [
    path('', views.home, name='home'),
    path('login/', views.zid_login, name='zid_login'),
    path('callback/', views.zid_callback, name='zid_callback'),
    path('logout/', views.zid_logout, name='zid_logout'),
    path('zid/refresh/', views.zid_refresh_token, name='zid_refresh'),
]