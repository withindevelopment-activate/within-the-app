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
    path("privacy-policy/", views.privacy_policy, name="privacy_policy"),
    path("data-deletion/", views.data_deletion, name="data_deletion"),

    path("marketing/", views.marketing_page, name="marketing"), # Marketing report section
    path("marketing_ready/", views.marketing_page_ready, name="marketing_ready"),
    path("process_marketing_report/", views.process_marketing_report, name="process_marketing_report"),

    path("save_tracking/", views.save_tracking, name="save_tracking"), # Tracking
    path("tracking_snippet.js", views.tracking_snippet, name="tracking_snippet"),
    path("view_tracking/", views.view_tracking, name="view_tracking"),
    
    path('zid/abandoned_carts/', views.abandoned_carts_page, name='abandoned_carts_page'),
    path('zid/customers/', views.customers_page, name='customers_page'),
    path("zid/customer-detail/<int:customer_id>/", views.customer_detail_api, name="customer_detail_api"),

    path('zid-webhook/product-update/', views.product_update, name='zid_product_update'),

    # --- Meta ---
    path("meta/login/", views.meta_login, name="meta_login"),
    path("meta/callback/", views.meta_callback, name="meta_callback"),
    path("meta/ad-accounts/", views.meta_ad_accounts, name="meta_ad_accounts"),
    path("meta/select-ad-account/<str:account_id>/", views.meta_select_ad_account, name="meta_select_ad_account"),
    path("meta/select-ad-account/", views.meta_select_ad_account, name="meta_select_ad_account"),  # fallback
    path("meta/campaigns/", views.meta_campaigns, name="meta_campaigns"),

    # --- Snapchat ---
    path('oauth/snapchat/login/', views.snapchat_login, name='snapchat_login'),
    path('oauth/snapchat/callback/', views.snapchat_callback, name='snapchat_callback'),
    path("snapchat/select-org/", views.select_organization, name="select_org"),
    path("snapchat/select-account/<str:org_id>/", views.select_ad_account, name="select_account"),
    path('snapchat/campaigns/', views.campaigns_overview, name='campaigns_overview'),

    # --- TikTok ---
    path("oauth/tiktok/login/", views.tiktok_login, name="tiktok_login"),
    path("oauth/tiktok/callback/", views.tiktok_callback, name="tiktok_callback"),
    path("tiktok/campaigns/", views.tiktok_campaigns, name="tiktok_campaigns"),
    path("tiktok/select-advertiser/<str:advertiser_id>/", views.tiktok_select_advertiser, name="tiktok_select_advertiser"),
    path("tiktok/select-advertiser/", views.tiktok_select_advertiser, name="tiktok_select_advertiser"),


    path("price-monitor/", views.view_price_monitor, name="price_monitor")
    # path("campaigns/overview/", views.campaigns_overview, name="campaigns_overview"),
]