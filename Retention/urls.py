from django.urls import path
from . import views

app_name = 'Retention'
urlpatterns = [
    path('zid-webhook/order-create/', views.adding_order_to_db, name='order_create_webhook'),
    # path('zid-webhook/order-update/', views.order_update_webhook, name='order_update_webhook'),
    # path('zid-webhook/customer-create/', views.customer_create_webhook, name='customer_create_webhook'),
    path('dashboard/', views.retention_dashboard, name='retention_dashboard'),
]