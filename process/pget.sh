#!/bin/sh
UID='104'
PASSWD='ganjituijian_!)@&_20141126'
TS=`date +%s`
key=`echo -n $UID$PASSWD$TS | md5sum | awk '{print $1}'`
url="http://tg.dns.ganji.com/api.php?c=FangQuery&a=getInfoByPuid&userid=104&time=$TS&key=$key"
puid=$1
fields='puid,city,district_id,price,huxing_shi,tab_system,zhuangxiu,title,street_id,major_category,latlng,ad_types,type,xiaoqu_id'
para="puid=$puid&fields=$fields"
curl -d "$para" "$url"
echo ""
echo $url
