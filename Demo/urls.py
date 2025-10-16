from django.urls import path
from . import views

app_name = 'Demo'
urlpatterns = [
    path('', views.home, name='home'),
    # ZID MAINS
    path('zid/login/', views.zid_login, name='zid_login'),
    path('zid/callback/', views.zid_callback, name='zid_callback'),
    path('zid/logout/', views.zid_logout, name='zid_logout'),
    path('zid/refresh/', views.zid_refresh_token, name='zid_refresh'),
    path('zid/match_google/', views.match_orders_with_analytics, name='match_google'),
    path('zid/orders/', views.orders_page, name='orders_page'),
    path('zid/products/', views.products_page, name='products_page'),
    
    path("search/", views.search_view, name="search"),

    path("marketing/", views.marketing_page, name="marketing"), # Marketing report section
    path("marketing_ready/", views.marketing_page_ready, name="marketing_ready"),
    path("process_marketing_report/", views.process_marketing_report, name="process_marketing_report"),

    path("save_tracking/", views.save_tracking, name="save_tracking"), # Tracking
    path("tracking_snippet.js", views.tracking_snippet, name="tracking_snippet"),
    path("view_tracking/", views.view_tracking, name="view_tracking"),
    
    path('zid/abandoned_carts/', views.abandoned_carts_page, name='abandoned_carts_page'),
    path('zid/customers/', views.customers_page, name='customers_page'),
    path("zid/customer-detail/<int:customer_id>/", views.customer_detail_api, name="customer_detail_api"),

    path('zid-webhook/product-update/', views.subscribe_store_to_product_update, name='zid_product_update'),
]