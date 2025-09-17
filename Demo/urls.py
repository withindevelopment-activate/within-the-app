from django.urls import path
from . import views

app_name = 'Demo'
urlpatterns = [
    # URL to start the OAuth flow (e.g., a button on your site)
    path('oauth/snapchat/login/', views.snapchat_login, name='snapchat_login'),
    path('oauth/snapchat/callback/', views.snapchat_callback, name='snapchat_callback'),
    path('snapchat/campaigns/', views.campaigns_overview, name='campaigns_overview'),

    path("oauth/tiktok/login/", views.tiktok_login, name="tiktok_login"),
    path("oauth/tiktok/callback/", views.tiktok_callback, name="tiktok_callback"),
    path("oauth/tiktok/refresh/", views.tiktok_refresh_token, name="tiktok_refresh"),
    path("tiktok/campaigns/", views.tiktok_campaigns, name="tiktok_campaigns"),

    path('', views.home, name='home'),
    path('login/', views.zid_login, name='zid_login'),
    path('callback/', views.zid_callback, name='zid_callback'),
    path('logout/', views.zid_logout, name='zid_logout'),
    path('zid/refresh/', views.zid_refresh_token, name='zid_refresh'),
    path('zid/match_google/', views.match_orders_with_analytics, name='match_google'),
    path('zid/orders/', views.orders_page, name='orders_page'),
    path('zid/products/', views.products_page, name='products_page'),
    path('zid/abandoned_carts/', views.abandoned_carts_page, name='abandoned_carts_page'),
    path('zid/customers/', views.customers_page, name='customers_page'),
    path("zid/customer-detail/<int:customer_id>/", views.customer_detail_api, name="customer_detail_api"),
    path("search/", views.search_view, name="search"),
    path("marketing/", views.marketing_page, name="marketing"),
    path("save_tracking/", views.save_tracking, name="save_tracking"),
    path("tracking_snippet.js", views.tracking_snippet, name="tracking_snippet"),
    path("view_tracking/", views.view_tracking, name="view_tracking"),
    path("utm-builder/", views.utm_builder, name="utm_builder"),

    # path("oauth/<str:provider>/login/", views.oauth_login, name="oauth_login"),
    # path("oauth/<str:provider>/callback/", views.oauth_callback, name="oauth_callback"),
    # path("snapchat/campaigns/", views.snapchat_campaigns_view, name="snapchat_campaigns"),
    # path("snapchat/choose_org/", views.choose_snapchat_org, name="choose_snapchat_org"),
    # path("snapchat/select_org/<str:org_id>/", views.select_snapchat_org, name="select_snapchat_org"),
]