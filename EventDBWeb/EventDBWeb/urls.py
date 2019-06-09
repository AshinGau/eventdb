"""EventDBWeb URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.11/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""
from django.conf.urls import url
from django.contrib import admin
from eventdbapp.views import *;

urlpatterns = [
    url(r'^admin/', admin.site.urls),
	url(r'^$', index, name='index'), 
	url(r'^index$',indexpage,name='indexpage'),
	url(r'^query$',query,name='query'),
	url(r'^queryHistory$',queryHistory,name='queryHistory'),
	url(r'^rundetail$',rundetail,name='rundetail'),
	url(r'^viewrun$',viewrun,name='viewrun'),
	url(r'^detail$',detail,name='detail'),
	url(r'^show$',show,name='show'),
	url(r'^fits$',viewfits,name='fits'),
	url(r'^rootOperator$',viewrootOperator,name='rootOperator'),
	url(r'^runInfo$',runInfo,name='runInfo'),
]
