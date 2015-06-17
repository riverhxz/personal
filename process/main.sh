#!/bin/bash

PROJ_HOME=$(cd "$(dirname "$0")/.."; pwd)
cd $PROJ_HOME

YESTERDAY=`date "+%F" -d '-1day'`
PV=data/app_fang_pv_$YESTERDAY
SHOW=data/app_fang_show_$YESTERDAY
MERGED_SAMPLE=tmp/merged_sample
POS_SAMPLE=tmp/pos_sample
NEG_SAMPLE=tmp/neg_sample
BLOCK_PRIFIX='history_trunk_'
BLOCK_DIR=tmp
#使用处理器个数
PROCESSER_NUM=3

cp /data1/ftp/upload/fang/app_fang_pv_daily $PV 
cp /data1/ftp/upload/fang/app_fang_list_daily $SHOW 
cat $PV | sort -n -k 1 | awk '{split($2,a,"@");split(a[2],b,"=");print $1" "b[2]}' | awk 'BEGIN{history="";pre=""}	{if(pre==$1){history=history","$2} else {print pre"\t"1"\t"history; history="";pre = $1;history=$2}} END{print pre"\t"1"\t"history}' > $POS_SAMPLE
cat $SHOW | sort -n -k 1  | process/filter.py > $NEG_SAMPLE
cat $POS_SAMPLE $NEG_SAMPLE | sort -n -k 1 > $MERGED_SAMPLE

#按行数分块
USER_COUNT=`wc -l $MERGED_SAMPLE|cut -d' ' -f1`
SPLIT_LINE=`expr $USER_COUNT / $PROCESSER_NUM`

split $MERGED_SAMPLE -l$SPLIT_LINE $BLOCK_DIR/$BLOCK_PRIFIX
BLOCKS=`ls $BLOCK_DIR/$BLOCK_PRIFIX*`

for block in $BLOCKS
do
	cat $block | process/processer_np.py &
done


