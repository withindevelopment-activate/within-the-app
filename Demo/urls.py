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
    path("process_marketing_report/", views.process_marketing_report, name="process_marketing_report"),

    path("save_tracking/", views.save_tracking, name="save_tracking"), # Tracking
    path("tracking_snippet.js", views.tracking_snippet, name="tracking_snippet"),
    path("purchase_snippet.js", views.purchase_snippet, name="purchase_snippet"),
    path("view_tracking/", views.view_tracking, name="view_tracking"),
    
    path('zid/abandoned_carts/', views.abandoned_carts_page, name='abandoned_carts_page'),
    path('zid/abandoned-carts/api/', views.abandoned_carts_api, name='abandoned_carts_api'),
    path('zid/abandoned-carts/api/<int:cart_id>/', views.abandoned_cart_detail_api, name='abandoned_cart_detail_api'),
    path('zid/customers/', views.customers_page, name='customers_page'),
    path("zid/customer-detail/<int:customer_id>/", views.customer_detail_api, name="customer_detail_api"),
    path('zid/customers/api/', views.customers_api,  name='customers_api'),


    path('zid-webhook/product-update/', views.product_update, name='zid_product_update'),

    # --- Meta ---
    path("meta/login/", views.meta_login, name="meta_login"),
    path("meta/callback/", views.meta_callback, name="meta_callback"),
    # path("meta/ad-accounts/", views.meta_ad_accounts, name="meta_ad_accounts"),
    path("meta/select-ad-account/<str:account_id>/", views.meta_select_ad_account, name="meta_select_ad_account"),
    path("meta/select-ad-account/", views.meta_select_ad_account, name="meta_select_ad_account"),  # fallback
    path("meta/campaigns/", views.meta_campaigns, name="meta_campaigns"),

    # --- Snapchat ---
    path('oauth/snapchat/login/', views.snapchat_login, name='snapchat_login'),
    path('oauth/snapchat/callback/', views.snapchat_callback, name='snapchat_callback'),
    path("snapchat/select-organization/", views.snapchat_select_organization, name="snapchat_select_organization"),
    path("snapchat/select-organization/<str:org_id>/", views.snapchat_select_organization, name="snapchat_select_organization_with_id"),
    path("snapchat/<str:org_id>/select-account/", views.snapchat_select_account, name="snapchat_select_account"),
    path("snapchat/<str:org_id>/select-account/<str:ad_account_id>/", views.snapchat_select_account, name="snapchat_select_account_with_id"),
    path('snapchat/campaigns/', views.campaigns_overview, name='campaigns_overview'),

    # --- TikTok ---
    path("oauth/tiktok/login/", views.tiktok_login, name="tiktok_login"),
    path("oauth/tiktok/callback/", views.tiktok_callback, name="tiktok_callback"),
    path("tiktok/campaigns/", views.tiktok_campaigns, name="tiktok_campaigns"),
    path("tiktok/select-advertiser/<str:advertiser_id>/", views.tiktok_select_advertiser, name="tiktok_select_advertiser"),
    path("tiktok/select-advertiser/", views.tiktok_select_advertiser, name="tiktok_select_advertiser"),
    # TikTok API endpoints
    path("tiktok/create-campaign/", views.tiktok_create_campaign, name="tiktok_create_campaign"),
    path("tiktok/create-adgroup/", views.tiktok_create_adgroup, name="tiktok_create_adgroup"),
    path("tiktok/create-ad/", views.tiktok_create_ad, name="tiktok_create_ad"),
    path("tiktok/upload-video/", views.upload_tiktok_video, name="tiktok_upload_video"),
    path("tiktok/upload-image/", views.upload_tiktok_image, name="tiktok_upload_image"),

    # get TikTok Targeting Options
    path('api/tiktok/locations/', views.get_tiktok_locations, name='tiktok_locations_api'),


    path("price-monitor/", views.view_price_monitor, name="price_monitor"),
    path("events-table/", views.events_table_view, name="events_table"),
    # path("campaigns/overview/", views.campaigns_overview, name="campaigns_overview"),

    ### The tracked customers
    path("view_tracked_customers/", views.view_tracked_customers, name="view_tracked_customers"),
    path("view_purchase_campaigns/", views.view_purchase_campaigns, name="view_purchase_campaigns"),
    # path("purchase-campaigns/spend/snapchat/", views.snapchat_spend, name="snapchat_spend"),
    # path("purchase-campaigns/spend/tiktok/", views.tiktok_spend, name="tiktok_spend"),
    # path("purchase-campaigns/spend/meta/", views.meta_spend, name="meta_spend"),
    path("update_campaign_products/", views.update_campaign_products, name="update_campaign_products"),
    path("view_platform_contributions/", views.view_platform_contributions, name="view_platform_contributions"),
    path("bridge/", views.tracking_bridge, name="tracking_bridge"),
    path("url-builder/", views.url_builder, name="url_builder"),
    ]
