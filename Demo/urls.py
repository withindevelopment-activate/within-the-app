from django.urls import path
from . import views

app_name = 'Demo'
urlpatterns = [
    path('', views.home, name='home'),
    path('login/', views.zid_login, name='zid_login'),
    path('callback/', views.zid_callback, name='zid_callback'),
    path('logout/', views.zid_logout, name='zid_logout'),
    path('zid/refresh/', views.zid_refresh_token, name='zid_refresh'),
    path('zid/match_google/', views.match_orders_with_analytics, name='match_google'),
    path('zid/orders/', views.orders_page, name='orders_page'),
    path('zid/products/', views.products_page, name='products_page'),
    path("search/", views.search_view, name="search"),
    path("marketing/", views.marketing_page, name="marketing"),
]