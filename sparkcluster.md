The columns I used to cluster: total_qtrly_wages taxable_qtrly_wages taxable_qtrly_wages taxable_qtrly_wages oty_qtrly_estabs_chg oty_qtrly_estabs_pct_chg oty_total_qtrly_wages_chg oty_total_qtrly_wages_pct_chg oty_taxable_qtrly_wages_chg oty_taxable_qtrly_wages_pct_chg oty_qtrly_contributions_chg oty_qtrly_contributions_pct_chg oty_avg_wkly_wage_chg oty_avg_wkly_wage_pct_chg labor unemploy

1. Two cluster result: total cost 0.045, cost distance 0.004

**prediction**|** avg(per\_dem)**|**stddev\_samp(per\_dem)**|**avg(per\_gop)**|**stddev\_samp(per\_gop)**
:-----:|:-----:|:-----:|:-----:|:-----:
1|0.38|0.24|0.55|0.26
0|0.32|0.17|0.63|0.17

cluster 0 dem: 0.14
cluster 0 gop: 0.77
cluster 1 dem: 0.5
cluster 1 gop: 0.5

Three cluster result: total cost 0.024, cost distance 0.003

**prediction**|** avg(per\_dem)**|**stddev\_samp(per\_dem)**|**avg(per\_gop)**|**stddev\_samp(per\_gop)**
:-----:|:-----:|:-----:|:-----:|:-----:
1|0.3845301691|0.2383138073|0.5496531818|0.2583720945
2|0.06651664648|0.03165655835|0.6295181836|0.1723916867
0|0.3221766948|0.1645625846|0.6243536611|0.1698547658

cluster 0 dem: 0.49612403100775193
cluster 0 gop: 0.9534883720930233
cluster 1 dem: 0.5
cluster 1 gop: 1.0
cluster 2 dem: 0.0
cluster 2 gop: 1.0

dem: a county with more than 30% dem

gop: a county with more than 30% gop

It appears that the republicans are everywhere ¯\_(ツ)_/¯

ORRRRRRR
My results improved when I got rid of all the new totally wages data and stuff, especially for two clusters. 
1. Two cluster result: total cost 0.002, cost distance 0.003

**prediction**|** avg(per\_dem)**|**stddev\_samp(per\_dem)**|**avg(per\_gop)**|**stddev\_samp(per\_gop)**
:-----:|:-----:|:-----:|:-----:|:-----:
1|0.57|NaN|0.43|NaN
0|0.32|0.17|0.63|0.17

cluster 0 dem: 0.13688212927756654
cluster 0 gop: 0.7680608365019012
cluster 1 dem: 1.0
cluster 1 gop: 0.0

2. Three cluster result: total cost 9.17e-4 cost distance 0.002
**prediction**|** avg(per\_dem)**|**stddev\_samp(per\_dem)**|**avg(per\_gop)**|**stddev\_samp(per\_gop)**
:-----:|:-----:|:-----:|:-----:|:-----:
1|0.33|0.15|0.61|0.16
2|0.22|0.23|0.74|0.24
0|0.25|0.20|0.70|0.19

cluster 0 dem: 0.11
cluster 0 gop: 0.86
cluster 1 dem: 0.14
cluster 1 gop: 0.75
cluster 2 dem: 0.29
cluster 2 gop: 1.0

